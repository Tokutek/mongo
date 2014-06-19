/**
 *    Copyright (C) 2012 10gen Inc.
 *    Copyright (C) 2013 Tokutek Inc.
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
#include "mongo/db/commands/server_status.h"
#include "mongo/db/crash.h"
#include "mongo/db/repl/bgsync.h"
#include "mongo/db/repl/rs_sync.h"
#include "mongo/base/counter.h"
#include "mongo/db/stats/timer_stats.h"

namespace mongo {
    void incRBID();
    BackgroundSync* BackgroundSync::s_instance = 0;
    boost::mutex BackgroundSync::s_mutex;

    //The number and time spent reading batches off the network
    static TimerStats getmoreReplStats;
    static ServerStatusMetricField<TimerStats> displayBatchesRecieved(
                                                    "repl.network.getmores",
                                                    &getmoreReplStats );
    //The oplog entries read via the oplog reader
    static Counter64 opsReadStats;
    static ServerStatusMetricField<Counter64> displayOpsRead( "repl.network.ops",
                                                                &opsReadStats );
    //The bytes read via the oplog reader
    static Counter64 networkByteStats;
    static ServerStatusMetricField<Counter64> displayBytesRead( "repl.network.bytes",
                                                                &networkByteStats );

    //The count of items in the buffer
    static Counter64 bufferCountGauge;
    static ServerStatusMetricField<Counter64> displayBufferCount( "repl.buffer.count",
                                                                &bufferCountGauge );
    //The size (bytes) of items in the buffer
    static Counter64 bufferSizeGauge;
    static ServerStatusMetricField<Counter64> displayBufferSize( "repl.buffer.sizeBytes",
                                                                &bufferSizeGauge );

    // Number and time of each ApplyOps worker pool round
    static TimerStats applyBatchStats;
    static ServerStatusMetricField<TimerStats> displayOpBatchesApplied(
                                                    "repl.apply.batches",
                                                    &applyBatchStats );
    //The oplog entries applied
    static Counter64 opsAppliedStats;
    static ServerStatusMetricField<Counter64> displayOpsApplied( "repl.apply.ops",
                                                                &opsAppliedStats );

    bool isRollbackRequired(OplogReader& r, uint64_t *lastTS) {
        string hn = r.conn()->getServerAddress();
        if (!r.more()) {
            // In vanilla Mongo, this happened for one of the
            // following reasons:
            //  - we were ahead of what we are syncing from (don't
            //    think that is possible anymore)
            //  - remote oplog is empty for some weird reason
            // in either case, if it (strangely) happens, we'll just return
            // and our caller will simply try again after a short sleep.
            log() << "replSet error empty query result from " << hn << " oplog, attempting rollback" << rsLog;
             return true;
        }

        BSONObj o = r.nextSafe();
        uint64_t ts = o["ts"]._numberLong();
        uint64_t lastHash = o["h"].numberLong();
        GTID gtid = getGTIDFromBSON("_id", o);

        if( !theReplSet->gtidManager->rollbackNeeded(gtid, ts, lastHash)) {
            log() << "Rollback NOT needed! Our GTID" << gtid << endl;
            return false;
        }

        log() << "Rollback needed! Our GTID" <<
            theReplSet->gtidManager->getLiveState().toString() <<
            " remote GTID: " << gtid.toString() << ". Attempting rollback." << rsLog;

        *lastTS = ts;
        return true;
    }

    BackgroundSync::BackgroundSync() : _opSyncShouldRun(false),
                                            _opSyncRunning(false),
                                            _seqCounter(0),
                                            _currentSyncTarget(NULL),
                                            _opSyncShouldExit(false),
                                            _opSyncInProgress(false),
                                            _applierShouldExit(false),
                                            _applierInProgress(false)
    {
    }

    BackgroundSync* BackgroundSync::get() {
        boost::unique_lock<boost::mutex> lock(s_mutex);
        if (s_instance == NULL && !inShutdown()) {
            s_instance = new BackgroundSync();
        }
        return s_instance;
    }

    void BackgroundSync::shutdown() {
        // first get producer thread to exit
        log() << "trying to shutdown bgsync" << rsLog;
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            _opSyncShouldExit = true;
            _opSyncShouldRun = false;
            _opSyncCanRunCondVar.notify_all();
        }
        // this does not need to be efficient
        // just sleep for periods of one second
        // until we see that we are no longer running 
        // the opSync thread
        log() << "waiting for opSync thread to end" << rsLog;
        while (_opSyncInProgress) {
            sleepsecs(1);
            log() << "still waiting for opSync thread to end... " << rsLog;
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
        log() << "waiting for applier thread to end" << rsLog;
        while (_applierInProgress) {
            sleepsecs(1);
            log() << "still waiting for applier thread to end..." << rsLog;
        }
        log() << "shutdown of bgsync complete" << rsLog;
    }

    void BackgroundSync::applierThread() {
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            _applierInProgress = true;
        }
        Client::initThread("applier");
        replLocalAuth();
        // we don't want the applier to be interrupted,
        // as it must finish work that it starts
        // done for github issues #770 and #771
        cc().setGloballyUninterruptible(true);
        applyOpsFromOplog();
        cc().shutdown();
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            _applierInProgress = false;
        }
    }

    void BackgroundSync::applyOpsFromOplog() {
        GTID lastLiveGTID;
        GTID lastUnappliedGTID;
        while (1) {
            try {
                BSONObj curr;
                {
                    boost::unique_lock<boost::mutex> lck(_mutex);
                    // wait until we know an item has been produced
                    while (_deque.size() == 0 && !_applierShouldExit) {
                        _queueDone.notify_all();
                        _queueCond.wait(lck);
                    }
                    if (_deque.size() == 0 && _applierShouldExit) {
                        return; 
                    }
                    curr = _deque.front();
                }
                GTID currEntry = getGTIDFromOplogEntry(curr);
                theReplSet->gtidManager->noteApplyingGTID(currEntry);
                // we must do applyTransactionFromOplog in a loop
                // because once we have called noteApplyingGTID, we must
                // continue until we are successful in applying the transaction.
                for (uint32_t numTries = 0; numTries <= 100; numTries++) {
                    try {
                        numTries++;
                        TimerHolder timer(&applyBatchStats);
                        applyTransactionFromOplog(curr);
                        opsAppliedStats.increment();
                        break;
                    }
                    catch (std::exception &e) {
                        log() << "exception during applying transaction from oplog: " << e.what() << endl;
                        log() << "oplog entry: " << curr.str() << endl;
                        if (numTries == 100) {
                            // something is really wrong if we fail 100 times, let's abort
                            dumpCrashInfo("100 errors applying oplog entry");
                            ::abort();
                        }
                        sleepsecs(1);
                    }
                }
                LOG(3) << "applied " << curr.toString(false, true) << endl;
                theReplSet->gtidManager->noteGTIDApplied(currEntry);

                {
                    boost::unique_lock<boost::mutex> lck(_mutex);
                    dassert(_deque.size() > 0);
                    _deque.pop_front();
                    bufferCountGauge.increment(-1);
                    bufferSizeGauge.increment(-curr.objsize());
                    
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
            catch (DBException& e) {
                sethbmsg(str::stream() << "db exception in producer on applier thread: " << e.toString());
                sleepsecs(2);
            }
            catch (std::exception& e2) {
                sethbmsg(str::stream() << "exception in producer on applier thread: " << e2.what());
                sleepsecs(2);
            }
        }
    }
    
    void BackgroundSync::producerThread() {
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            _opSyncInProgress = true;
        }
        Client::initThread("rsBackgroundSync");
        replLocalAuth();
        uint32_t timeToSleep = 0;

        while (!_opSyncShouldExit) {
            try {
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
                        // do it in 2 second intervals. This is simply
                        // defensive. Should we ever have a bug where
                        // we are not getting signaled properly,
                        // at worst, we delay waking up this thread for 2
                        // seconds
                        _opSyncCanRunCondVar.timed_wait(
                            lck,
                            boost::posix_time::milliseconds(2000)
                            );
                    }

                    // notify other threads that we are running
                    _opSyncRunningCondVar.notify_all();
                    _opSyncRunning = true;
                }
                // get out if we need to
                if (_opSyncShouldExit) { break; }

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
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            _opSyncRunning = false;
            _opSyncInProgress = false;
        }
    }

    void BackgroundSync::handleSlaveDelay(uint64_t opTimestamp) {
        dassert(_opSyncRunning);
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
            }
            // reset currTime
            currTime = curTimeMillis64();
        }
    }

    // returns number of seconds to sleep, if any
    uint32_t BackgroundSync::produce() {

        // normally msgCheckNewState gets called periodically, but in a single node repl set
        // there are no heartbeat threads, so we do it here to be sure.  this is relevant if the
        // singleton member has done a stepDown() and needs to come back up.
        if (theReplSet->config().members.size() == 1 &&
            theReplSet->myConfig().potentiallyHot()) {
            Manager* mgr = theReplSet->mgr;
            // When would mgr be null?  During replsettest'ing, in which case we should
            // fall through and actually apply ops as if we were a real secondary.
            if (mgr) {
                mgr->send(boost::bind(&Manager::msgCheckNewState, theReplSet->mgr));
                // There should never be ops to sync in a 1-member set, anyway
                return 1;
            }
        }

        bool acknowledgingWrites = theReplSet->gtidManager->canAcknowledgeGTID();
        OplogReader r(acknowledgingWrites /* doHandshake */);

        // find a target to sync from the last op time written
        getOplogReader(&r);

        // no server found
        {
            boost::unique_lock<boost::mutex> lock(_mutex);

            if (_currentSyncTarget == NULL) {
                // if there is no one to sync from
                return 1; //sleep one second
            }
        }
        GTID lastGTIDFetched = theReplSet->gtidManager->getLiveState();
        r.tailingQueryGTE(rsoplog, lastGTIDFetched);

        // if target cut connections between connecting and querying (for
        // example, because it stepped down) we might not have a cursor
        if (!r.haveCursor()) {
            return 0;
        }

        try {
            // this method may actually run rollback, yes, the name is bad
            uint64_t ts;
            if (isRollbackRequired(r, &ts)) {
                // sleep 2 seconds and try again. (The 2 is arbitrary).
                // If we are not fatal, then we will keep trying to sync
                // from another machine
                runRollback(r, ts);
                return 2;
            }
        }
        catch (RollbackOplogException& re){
            // we attempted a rollback and failed, we must go fatal.
            log() << "Caught a RollbackOplogException during rollback, going fatal" << rsLog;
            theReplSet->fatal();
            return 2; // 2 is arbitrary, if we are going fatal, we are done
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
                    // now that we have no more in the current batch,
                    // that means the next call to more() will contact the server.
                    // It is this server contact that may or may not acknowledge the write,
                    // depending on the value of acknowledgingWrites set above.
                    // So, we check if the state of our acknowledgement has changed
                    // If so, we return so that a new connection and OplogReader is created,
                    // with the correct value for acknowledgingWrites
                    bool canAck = theReplSet->gtidManager->canAcknowledgeGTID();
                    if (canAck != acknowledgingWrites) {
                        return 0;
                    }
                    // check to see if we have a request to sync
                    // from a specific target. If so, get out so that
                    // we can restart the act of syncing and
                    // do so from the correct target
                    if (theReplSet->gotForceSync()) {
                        return 0;
                    }

                    verify(!theReplSet->isPrimary());

                    if (shouldChangeSyncTarget()) {
                        return 0;
                    }
                    //record time for each getmore
                    {
                        uint64_t currentSeq;
                        {
                            // check if we should bail out
                            boost::unique_lock<boost::mutex> lck(_mutex);
                            currentSeq = _seqCounter;
                            if (!_opSyncShouldRun) {
                                return 0;
                            }
                            _opSyncRunning = false;
                            // notify other threads that we are running
                            _opSyncRunningCondVar.notify_all();
                        }
                        TimerHolder batchTimer(&getmoreReplStats);
                        r.more();
                        {
                            // check if we should bail out
                            boost::unique_lock<boost::mutex> lck(_mutex);
                            // if we should not run, or notice that
                            // replication has stopped and restarted since the call to
                            // r.more(), return
                            if (!_opSyncShouldRun || _seqCounter > currentSeq) {
                                return 0;
                            }
                            _opSyncRunning = true;
                            // notify other threads that we are running
                            _opSyncRunningCondVar.notify_all();
                        }
                    }
                    //increment
                    networkByteStats.increment(r.currentBatchMessageSize());

                }

                if (!r.moreInCurrentBatch()) {
                    break;
                }

                // This is the operation we have received from the target
                // that we must put in our oplog with an applied field of false
                BSONObj o = r.nextSafe().getOwned();
                opsReadStats.increment();
                LOG(3) << "replicating " << o.toString(false, true) << " from " << _currentSyncTarget->fullName() << endl;
                uint64_t ts = o["ts"]._numberLong();

                // now that we have the element in o, let's check
                // if there a delay is required (via slaveDelay) before
                // writing it to the oplog
                if (theReplSet->myConfig().slaveDelay > 0) {
                    handleSlaveDelay(ts);
                    {
                        boost::unique_lock<boost::mutex> lck(_mutex);
                        if (!_opSyncShouldRun) {
                            break;
                        }
                    }
                }

                {
                    Timer timer;
                    bool bigTxn = false;
                    {
                        Client::Transaction transaction(DB_SERIALIZABLE);
                        replicateFullTransactionToOplog(o, r, &bigTxn);
                        // we are operating as a secondary. We don't have to fsync
                        transaction.commit(DB_TXN_NOSYNC);
                    }
                    {
                        GTID currEntry = getGTIDFromOplogEntry(o);
                        uint64_t lastHash = o["h"].numberLong();
                        boost::unique_lock<boost::mutex> lock(_mutex);
                        // update counters
                        theReplSet->gtidManager->noteGTIDAdded(currEntry, ts, lastHash);
                        // notify applier thread that data exists
                        if (_deque.size() == 0) {
                            _queueCond.notify_all();
                        }
                        _deque.push_back(o);
                        bufferCountGauge.increment();
                        bufferSizeGauge.increment(o.objsize());
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
                        if (bigTxn) {
                            // if we have a large transaction, we don't want
                            // to let it pile up. We want to process it immedietely
                            // before processing anything else.
                            while (_deque.size() > 0) {
                                _queueDone.wait(lock);
                            }
                        }
                    }
                }
            } // end while

            if (shouldChangeSyncTarget()) {
                return 0;
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

    bool BackgroundSync::shouldChangeSyncTarget() {
        boost::unique_lock<boost::mutex> lock(_mutex);

        // is it even still around?
        if (!_currentSyncTarget || !_currentSyncTarget->hbinfo().hbstate.readable()) {
            return true;
        }

        // check other members: is any member's optime more than 30 seconds ahead of the guy we're
        // syncing from?
        return theReplSet->shouldChangeSyncTarget(_currentSyncTarget->hbinfo().opTime);
    }

    void BackgroundSync::getOplogReader(OplogReader* r) {
        const Member* target = NULL;

        verify(r->conn() == NULL);
        while ((target = theReplSet->getMemberToSyncTo()) != NULL) {
            string current = target->fullName();

            if (!r->connect(current)) {
                LOG(2) << "replSet can't connect to " << current << " to read operations" << rsLog;
                r->resetConnection();
                theReplSet->veto(current);
                continue;
            }

            {
                boost::unique_lock<boost::mutex> lock(_mutex);
                _currentSyncTarget = target;
            }

            return;
        }

        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            _currentSyncTarget = NULL;
        }
    }

    void BackgroundSync::runRollback(OplogReader& r, uint64_t oplogTS) {
        // starting from ourLast, we need to read the remote oplog
        // backwards until we find an entry in the remote oplog
        // that has the same GTID, timestamp, and hash as
        // what we have in our oplog. If we don't find one that is within
        // some reasonable timeframe, then we go fatal
        GTID ourLast = theReplSet->gtidManager->getLiveState();
        GTID idToRollbackTo;
        uint64_t rollbackPointTS = 0;
        uint64_t rollbackPointHash = 0;
        incRBID();
        try {
            shared_ptr<DBClientCursor> rollbackCursor = r.getRollbackCursor(ourLast);
            uassert(17350, "rollback failed to get a cursor to start reading backwards from.", rollbackCursor.get());
            while (rollbackCursor->more()) {
                BSONObj remoteObj = rollbackCursor->next();
                GTID remoteGTID = getGTIDFromBSON("_id", remoteObj);
                uint64_t remoteTS = remoteObj["ts"]._numberLong();
                uint64_t remoteLastHash = remoteObj["h"].numberLong();
                if (remoteTS + 1800*1000 < oplogTS) {
                    log() << "Rollback takes us too far back, throwing exception. remoteTS: " << remoteTS << " oplogTS: " << oplogTS << rsLog;
                    throw RollbackOplogException("replSet rollback too long a time period for a rollback (at least 30 minutes).");
                    break;
                }
                //now try to find an entry in our oplog with that GTID
                BSONObjBuilder localQuery;
                BSONObj localObj;
                addGTIDToBSON("_id", remoteGTID, localQuery);
                bool foundLocally = false;
                {
                    LOCK_REASON(lockReason, "repl: looking up oplog entry for rollback");
                    Client::ReadContext ctx(rsoplog, lockReason);
                    Client::Transaction transaction(DB_SERIALIZABLE);
                    foundLocally = Collection::findOne(rsoplog, localQuery.done(), localObj);
                    transaction.commit();
                }
                if (foundLocally) {
                    GTID localGTID = getGTIDFromBSON("_id", localObj);
                    uint64_t localTS = localObj["ts"]._numberLong();
                    uint64_t localLastHash = localObj["h"].numberLong();
                    if (localLastHash == remoteLastHash &&
                        localTS == remoteTS &&
                        GTID::cmp(localGTID, remoteGTID) == 0
                        )
                    {
                        idToRollbackTo = localGTID;
                        rollbackPointTS = localTS;
                        rollbackPointHash = localLastHash;
                        log() << "found id to rollback to " << idToRollbackTo << rsLog;
                        break;
                    }
                }
            }
            // At this point, either we have found the point to try to rollback to,
            // or we have determined that we cannot rollback
            if (idToRollbackTo.isInitial()) {
                // we cannot rollback
                throw RollbackOplogException("could not find ID to rollback to");
            }
        }
        catch (DBException& e) {
            log() << "Caught DBException during rollback " << e.toString() << rsLog;
            throw RollbackOplogException("DBException while trying to find ID to rollback to: " + e.toString());
        }
        catch (std::exception& e2) {
            log() << "Caught std::exception during rollback " << e2.what() << rsLog;
            throw RollbackOplogException(str::stream() << "Exception while trying to find ID to rollback to: " << e2.what());
        }

        // proceed with the rollback to point idToRollbackTo
        // probably ought to grab a global write lock while doing this
        // I don't think we want oplog cursors reading from this machine
        // while we are rolling back. Or at least do something to protect against this

        // first, let's get all the operations that are being applied out of the way,
        // we don't want to rollback an item in the oplog while simultaneously,
        // the applier thread is applying it to the oplog
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            while (_deque.size() > 0) {
                log() << "waiting for applier to finish work before doing rollback " << rsLog;
                _queueDone.wait(lock);
            }
            verifySettled();
        }

        // now let's tell the system we are going to rollback, to do so,
        // abort live multi statement transactions, invalidate cursors, and
        // change the state to RS_ROLLBACK
        {
            // so we know nothing is simultaneously occurring
            RWLockRecursive::Exclusive e(operationLock);
            LOCK_REASON(lockReason, "repl: killing all operations for rollback");
            Lock::GlobalWrite lk(lockReason);
            ClientCursor::invalidateAllCursors();
            Client::abortLiveTransactions();
            theReplSet->goToRollbackState();
        }

        try {
            // now that we are settled, we have to take care of the GTIDManager
            // and the repl info thread.
            // We need to reset the state of the GTIDManager to the point
            // we intend to rollback to, and we need to make sure that the repl info thread
            // has captured this information.
            theReplSet->gtidManager->resetAfterInitialSync(
                idToRollbackTo,
                rollbackPointTS,
                rollbackPointHash
                );
            // now force an update of the repl info thread
            theReplSet->forceUpdateReplInfo();

            // at this point, everything should be settled, the applier should
            // have nothing left (and remain that way, because this is the only
            // thread that can put work on the applier). Now we can rollback
            // the data.
            while (true) {
                BSONObj o;
                {
                    LOCK_REASON(lockReason, "repl: checking for oplog data");
                    Lock::DBRead lk(rsoplog, lockReason);
                    Client::Transaction txn(DB_SERIALIZABLE);
                    // if there is nothing in the oplog, break
                    o = getLastEntryInOplog();
                    if( o.isEmpty() ) {
                        break;
                    }
                }
                GTID lastGTID = getGTIDFromBSON("_id", o);
                // if we have rolled back enough, break from while loop
                if (GTID::cmp(lastGTID, idToRollbackTo) <= 0) {
                    dassert(GTID::cmp(lastGTID, idToRollbackTo) == 0);
                    break;
                }
                rollbackTransactionFromOplog(o, true);
            }
            theReplSet->leaveRollbackState();
        }
        catch (DBException& e) {
            log() << "Caught DBException during rollback " << e.toString() << rsLog;
            throw RollbackOplogException("DBException while trying to run rollback: " + e.toString());
        }
        catch (std::exception& e2) {
            log() << "Caught std::exception during rollback " << e2.what() << rsLog;
            throw RollbackOplogException(str::stream() << "Exception while trying to run rollback: " << e2.what());
        }
        
    }

    const Member* BackgroundSync::getSyncTarget() {
        boost::unique_lock<boost::mutex> lock(_mutex);
        return _currentSyncTarget;
    }

    // does some sanity checks before finishing starting and stopping the opsync 
    // thread that we are in a decent state
    //
    // called with _mutex held
    void BackgroundSync::verifySettled() {
        // if the background sync has yet to be fully started,
        // no need to run this, we are still in initialization
        // of the replset. This can happen if
        // during initialization, after we start the manager, we
        // get a new config before we have fully started replication
        if (!_applierInProgress) {
            return;
        }
        verify(_deque.size() == 0);
        // do a sanity check on the GTID Manager
        GTID lastLiveGTID;
        GTID lastUnappliedGTID;
        theReplSet->gtidManager->getLiveGTIDs(
            &lastLiveGTID, 
            &lastUnappliedGTID
            );
        log() << "last GTIDs: " << 
            lastLiveGTID.toString() << " " << 
            lastUnappliedGTID.toString() << " " << endl;
        verify(GTID::cmp(lastUnappliedGTID, lastLiveGTID) == 0);

        GTID minLiveGTID;
        GTID minUnappliedGTID;
        theReplSet->gtidManager->getMins(
            &minLiveGTID, 
            &minUnappliedGTID
            );
        log() << "min GTIDs: " << 
            minLiveGTID.toString() << " " <<
            minUnappliedGTID.toString() << rsLog;
        verify(GTID::cmp(minUnappliedGTID, minLiveGTID) == 0);
    }

    void BackgroundSync::stopOpSyncThread() {
        boost::unique_lock<boost::mutex> lock(_mutex);
        _seqCounter++;
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
        _seqCounter++;
        // if we are shutting down, don't start replication
        // otherwise, we will be stuck waiting on _opSyncRunning
        // to go true, and it may never go true
        if (!_opSyncShouldExit) {
            verifySettled();

            _opSyncShouldRun = true;
            _opSyncCanRunCondVar.notify_all();
            // we trust the fact that this thread will signal
            // the producer to wake up
            // No need to wait for it, as the producer may be
            // sleeping, or hung on a r.more() call in ::produce
        }
    }

} // namespace mongo
