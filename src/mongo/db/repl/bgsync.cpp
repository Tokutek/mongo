/**
 *    Copyright (C) 2012 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "mongo/pch.h"

#include "mongo/db/client.h"
#include "mongo/db/commands/fsync.h"
#include "mongo/db/repl/bgsync.h"
#include "mongo/db/repl/rs_sync.h"

namespace mongo {
    BackgroundSync* BackgroundSync::s_instance = 0;
    boost::mutex BackgroundSync::s_mutex;


    BackgroundSync::BackgroundSync() : _opSyncShouldRun(false),
                                            _opSyncRunning(false),
                                            _currentSyncTarget(NULL),
                                            _opSyncShouldExit(false),
                                            _opSyncInProgress(false),
                                            _applierShouldExit(false),
                                            _applierInProgress(false)
    {
    }

    BackgroundSync::QueueCounter::QueueCounter() : waitTime(0) {
    }

    BackgroundSync* BackgroundSync::get() {
        boost::unique_lock<boost::mutex> lock(s_mutex);
        if (s_instance == NULL && !inShutdown()) {
            s_instance = new BackgroundSync();
        }
        return s_instance;
    }

    BSONObj BackgroundSync::getCounters() {
        BSONObjBuilder counters;
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            counters.appendIntOrLL("waitTimeMs", _queueCounter.waitTime);
            uint32_t size = _deque.size();
            counters.append("numElems", size);
        }
        return counters.obj();
    }

    void BackgroundSync::shutdown() {
        // first get producer thread to exit
        log() << "trying to shutdown bgsync" << endl;
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            _opSyncShouldExit = true;
            _opSyncCanRunCondVar.notify_all();
        }
        // this does not need to be efficient
        // just sleep for periods of one second
        // until we see that we are no longer running 
        // the opSync thread
        log() << "waiting for opSync thread to end" << endl;
        while (_opSyncInProgress) {
            sleepsecs(1);
            log() << "still waiting for opSync thread to end... " << endl;
        }

        // at this point, the opSync thread should be done
        _queueCond.notify_all();
        
        // now get applier thread to exit
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            _applierShouldExit = true;
            _queueCond.notify_all();
        }
        // same reasoning as with _opSyncInProgress above
        log() << "waiting for applier thread to end" << endl;
        while (_applierInProgress) {
            sleepsecs(1);
            log() << "still waiting for applier thread to end..." << endl;
        }
        log() << "shutdown of bgsync complete" << endl;
    }

    void BackgroundSync::applierThread() {
        _applierInProgress = true;
        Client::initThread("applier");
        replLocalAuth();
        applyOpsFromOplog();
        cc().shutdown();
        _applierInProgress = false;
    }

    void BackgroundSync::applyOpsFromOplog() {
        GTID lastLiveGTID;
        GTID lastUnappliedGTID;
        while (1) {
            BSONObj curr;
            {
                boost::unique_lock<boost::mutex> lck(_mutex);
                // wait until we know an item has been produced
                while (_deque.size() == 0 && !_applierShouldExit) {
                    _queueDone.notify_all();
                    _queueCond.wait(_mutex);
                }
                if (_deque.size() == 0 && _applierShouldExit) {
                    return; 
                }
                curr = _deque.front();
            }
            GTID currEntry = getGTIDFromOplogEntry(curr);
            theReplSet->gtidManager->noteApplyingGTID(currEntry);
            applyTransactionFromOplog(curr);

            {
                boost::unique_lock<boost::mutex> lck(_mutex);
                // I don't recall if noteGTIDApplied needs to be called within _mutex
                theReplSet->gtidManager->noteGTIDApplied(currEntry);
                dassert(_deque.size() > 0);
                _deque.pop_front();
                
                // this is a flow control mechanism, with bad numbers
                // hard coded for now just to get something going.
                // If the opSync thread notices that we have over 20000
                // transactions in the queue, it waits until we get below
                // 10000. This is where we signal that we have gotten there
                // Once we have spilling of transactions working, this
                // logic will need to be redone
                if (_deque.size() == 10000) {
                    _queueCond.notify_all();
                }
            }
        }
    }
    
    void BackgroundSync::producerThread() {
        _opSyncInProgress = true;
        Client::initThread("rsBackgroundSync");
        replLocalAuth();
        uint32_t timeToSleep = 0;

        while (!_opSyncShouldExit) {
            if (timeToSleep) {
                {
                    boost::unique_lock<boost::mutex> lck(_mutex);
                    _opSyncRunning = false;
                    // notify other threads that we are not running
                    _opSyncRunningCondVar.notify_all();
                }
                for (uint32_t i = 0; i < timeToSleep; i++) {
                    sleepsecs(1);
                    // get out if we need to
                    if (_opSyncShouldExit) { break; }
                }
                timeToSleep = 0;
            }
            // get out if we need to
            if (_opSyncShouldExit) { break; }

            {
                boost::unique_lock<boost::mutex> lck(_mutex);
                _opSyncRunning = false;

                while (!_opSyncShouldRun && !_opSyncShouldExit) {
                    // notify other threads that we are not running
                    _opSyncRunningCondVar.notify_all();
                    // wait for permission that we can run
                    _opSyncCanRunCondVar.wait(lck);
                }

                // notify other threads that we are running
                _opSyncRunningCondVar.notify_all();
                _opSyncRunning = true;
            }
            // get out if we need to
            if (_opSyncShouldExit) { break; }

            try {
                MemberState state = theReplSet->state();
                if (state.fatal() || state.startup()) {
                    timeToSleep = 5;
                    continue;
                }
                // this does the work of reading a remote oplog
                // and writing it to our oplog
                timeToSleep = produce();
            }
            catch (DBException& e) {
                sethbmsg(str::stream() << "db exception in producer: " << e.toString());
                timeToSleep = 10;
            }
            catch (std::exception& e2) {
                sethbmsg(str::stream() << "exception in producer: " << e2.what());
                timeToSleep = 10;
            }
        }

        cc().shutdown();
        _opSyncRunning = false; // this is racy, but who cares, we are shutting down
        _opSyncInProgress = false;
    }

    void BackgroundSync::handleSlaveDelay(uint64_t opTimestamp) {
        uint64_t slaveDelayMillis = theReplSet->myConfig().slaveDelay * 1000;
        uint64_t currTime = curTimeMillis64();
        uint64_t timeOpShouldBeApplied = opTimestamp + slaveDelayMillis;
        while (currTime < timeOpShouldBeApplied) {
            uint64_t sleepTime = (timeOpShouldBeApplied - currTime);
            // let's sleep for at most one second
            sleepmillis((sleepTime < 1000) ? sleepTime : 1000);        
            // check if we should bail out, as we don't want to 
            // sleep the whole time possibly long delay time
            // if we see we should be stopping
            {
                boost::unique_lock<boost::mutex> lck(_mutex);
                if (!_opSyncShouldRun) {
                    break;
                }
                // reset currTime
                currTime = curTimeMillis64();
            }
        }
    }

    // returns number of seconds to sleep, if any
    uint32_t BackgroundSync::produce() {
        // this oplog reader does not do a handshake because we don't want the server it's syncing
        // from to track how far it has synced
        OplogReader r(true /* doHandshake */);

        // find a target to sync from the last op time written
        getOplogReader(r);

        // no server found
        GTID lastGTIDFetched = theReplSet->gtidManager->getLiveState();
        {
            boost::unique_lock<boost::mutex> lock(_mutex);

            if (_currentSyncTarget == NULL) {
                // if there is no one to sync from
                return 1; //sleep one second
            }
            r.tailingQueryGTE(rsoplog, lastGTIDFetched);
        }

        // if target cut connections between connecting and querying (for
        // example, because it stepped down) we might not have a cursor
        if (!r.haveCursor()) {
            return 0;
        }

        if (isRollbackRequired(r)) {
            return 0;
        }

        while (!_opSyncShouldExit) {
            while (!_opSyncShouldExit) {
                {
                    // check if we should bail out
                    boost::unique_lock<boost::mutex> lck(_mutex);
                    if (!_opSyncShouldRun) {
                        return 0;
                    }
                }
                if (!r.moreInCurrentBatch()) {
                    // check to see if we have a request to sync
                    // from a specific target. If so, get out so that
                    // we can restart the act of syncing and
                    // do so from the correct target
                    if (theReplSet->gotForceSync()) {
                        return 0;
                    }

                    verify(!theReplSet->isPrimary());

                    {
                        boost::unique_lock<boost::mutex> lock(_mutex);
                        if (!_currentSyncTarget || !_currentSyncTarget->hbinfo().hbstate.readable()) {
                            return 0;
                        }
                    }

                    r.more();
                }

                if (!r.more()) {
                    break;
                }

                // This is the operation we have received from the target
                // that we must put in our oplog with an applied field of false
                BSONObj o = r.nextSafe().getOwned();
                uint64_t ts = o["ts"]._numberLong();

                // now that we have the element in o, let's check
                // if there a delay is required (via slaveDelay) before
                // writing it to the oplog
                if (theReplSet->myConfig().slaveDelay > 0) {
                    handleSlaveDelay(ts);
                }
                
                Timer timer;
                {
                    Client::ReadContext ctx(rsoplog);
                    Client::Transaction transaction(DB_SERIALIZABLE);
                    replicateTransactionToOplog(o);                    
                    // we are operating as a secondary. We don't have to fsync
                    transaction.commit(DB_TXN_NOSYNC);
                }
                {
                    GTID currEntry = getGTIDFromOplogEntry(o);
                    uint64_t lastHash = o["h"].numberLong();
                    boost::unique_lock<boost::mutex> lock(_mutex);
                    // update counters
                    theReplSet->gtidManager->noteGTIDAdded(currEntry, ts, lastHash);
                    _queueCounter.waitTime += timer.millis();
                    // notify applier thread that data exists
                    if (_deque.size() == 0) {
                        _queueCond.notify_all();
                    }
                    _deque.push_back(o);
                    // this is a flow control mechanism, with bad numbers
                    // hard coded for now just to get something going.
                    // If the opSync thread notices that we have over 20000
                    // transactions in the queue, it waits until we get below
                    // 10000. This is where we wait if we get too high
                    // Once we have spilling of transactions working, this
                    // logic will need to be redone
                    if (_deque.size() > 20000) {
                        _queueCond.wait(lock);
                    }
                }
            } // end while

            {
                boost::unique_lock<boost::mutex> lock(_mutex);
                if (!_currentSyncTarget || !_currentSyncTarget->hbinfo().hbstate.readable()) {
                    return 0;
                }
            }


            r.tailCheck();
            if( !r.haveCursor() ) {
                LOG(1) << "replSet end opSync pass" << rsLog;
                return 0;
            }

            // looping back is ok because this is a tailable cursor
        }
        return 0;
    }

    bool BackgroundSync::isStale(OplogReader& r, BSONObj& remoteOldestOp) {
        remoteOldestOp = r.findOne(rsoplog, Query());
        GTID remoteOldestGTID = getGTIDFromBSON("_id", remoteOldestOp);
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            GTID currLiveState = theReplSet->gtidManager->getLiveState();
            if (GTID::cmp(currLiveState, remoteOldestGTID) < 0) {
                return true;
            }
        }
        return false;
    }

    void BackgroundSync::getOplogReader(OplogReader& r) {
        Member *target = NULL, *stale = NULL;
        BSONObj oldest;

        verify(r.conn() == NULL);
        while ((target = theReplSet->getMemberToSyncTo()) != NULL) {
            string current = target->fullName();

            if (!r.connect(current)) {
                LOG(2) << "replSet can't connect to " << current << " to read operations" << rsLog;
                r.resetConnection();
                theReplSet->veto(current);
                continue;
            }

            if (isStale(r, oldest)) {
                r.resetConnection();
                theReplSet->veto(current, 600);
                stale = target;
                continue;
            }

            // if we made it here, the target is up and not stale
            {
                boost::unique_lock<boost::mutex> lock(_mutex);
                _currentSyncTarget = target;
            }

            return;
        }

        // the only viable sync target was stale
        if (stale) {
            GTID remoteOldestGTID = getGTIDFromBSON("_id", oldest);
            theReplSet->goStale(stale, remoteOldestGTID);
            sleepsecs(120);
        }

        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            _currentSyncTarget = NULL;
        }
    }

    bool BackgroundSync::isRollbackRequired(OplogReader& r) {
        string hn = r.conn()->getServerAddress();
        if (!r.more()) {
            ::abort();
            return true;
        }

        BSONObj o = r.nextSafe();
        uint64_t ts = o["ts"]._numberLong();
        uint64_t lastHash = o["h"].numberLong();
        GTID gtid = getGTIDFromBSON("_id", o);
        
        if( theReplSet->gtidManager->rollbackNeeded(gtid, ts, lastHash)) {
            log() << "rollback needed!" << rsLog;
            ::abort();
            return true;
        }

        return false;
    }

    Member* BackgroundSync::getSyncTarget() {
        boost::unique_lock<boost::mutex> lock(_mutex);
        return _currentSyncTarget;
    }

    // does some sanity checks before finishing starting and stopping the opsync 
    // thread that we are in a decent state
    //
    // called with _mutex held
    void BackgroundSync::verifySettled() {
        verify(_deque.size() == 0);
        // do a sanity check on the GTID Manager
        GTID lastLiveGTID;
        GTID lastUnappliedGTID;
        theReplSet->gtidManager->getLiveGTIDs(
            &lastLiveGTID, 
            &lastUnappliedGTID
            );
        verify(GTID::cmp(lastUnappliedGTID, lastLiveGTID) == 0);

        GTID minLiveGTID;
        GTID minUnappliedGTID;
        theReplSet->gtidManager->getMins(
            &minLiveGTID, 
            &minUnappliedGTID
            );
        verify(GTID::cmp(minUnappliedGTID, minLiveGTID) == 0);
    }

    void BackgroundSync::stopOpSyncThread() {
        boost::unique_lock<boost::mutex> lock(_mutex);
        _opSyncShouldRun = false;
        while (_opSyncRunning) {
            _opSyncRunningCondVar.wait(lock);
        }
        // sanity checks
        verify(!_opSyncShouldRun);

        // wait for all things to be applied
        while (_deque.size() > 0) {
            _queueDone.wait(lock);
        }

        verifySettled();
    }

    void BackgroundSync::startOpSyncThread() {
        boost::unique_lock<boost::mutex> lock(_mutex);
        verifySettled();

        _opSyncShouldRun = true;
        _opSyncCanRunCondVar.notify_all();
        while (!_opSyncRunning) {
            _opSyncRunningCondVar.wait(lock);
        }
        // sanity check that no one has changed this variable
        verify(_opSyncShouldRun);
    }

} // namespace mongo
