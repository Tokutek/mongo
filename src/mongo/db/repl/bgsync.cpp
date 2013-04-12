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

    BackgroundSyncInterface::~BackgroundSyncInterface() {}

    size_t getSize(const BSONObj& o) {
        return o.objsize();
    }

    BackgroundSync::BackgroundSync() : _buffer(256*1024*1024, &getSize),
                                       _lastOpTimeFetched(0, 0),
                                       _pause(true),
                                       _currentSyncTarget(NULL),
                                       _consumedOpTime(0, 0) {
    }

    BackgroundSync::QueueCounter::QueueCounter() : waitTime(0), numElems(0) {
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
            counters.append("numElems", _queueCounter.numElems);
        }
        // _buffer is protected by its own mutex
        counters.appendNumber("numBytes", _buffer.size());
        return counters.obj();
    }

    void BackgroundSync::shutdown() {
        notify();
    }

    void BackgroundSync::notify() {
        boost::unique_lock<boost::mutex> lock(s_mutex);
        if (s_instance == NULL) {
            return;
        }

        boost::unique_lock<boost::mutex> opLock(s_instance->_lastOpMutex);
        s_instance->_lastOpCond.notify_all();
    }

    void BackgroundSync::producerThread() {
        Client::initThread("rsBackgroundSync");
        replLocalAuth();

        while (!inShutdown()) {
            if (!theReplSet) {
                log() << "replSet warning did not receive a valid config yet, sleeping 20 seconds " << rsLog;
                sleepsecs(20);
                continue;
            }

            try {
                _producerThread();
            }
            catch (DBException& e) {
                sethbmsg(str::stream() << "db exception in producer: " << e.toString());
                sleepsecs(10);
            }
            catch (std::exception& e2) {
                sethbmsg(str::stream() << "exception in producer: " << e2.what());
                sleepsecs(60);
            }
        }

        cc().shutdown();
    }

    void BackgroundSync::_producerThread() {
        MemberState state = theReplSet->state();

        // we want to pause when the state changes to primary
        if (state.primary()) {
            if (!_pause) {
                stop();
            }
            sleepsecs(1);
            return;
        }

        if (state.fatal() || state.startup()) {
            sleepsecs(5);
            return;
        }

        // if this member has an empty oplog, we cannot start syncing
        if (theReplSet->lastOpTimeWritten.isNull()) {
            sleepsecs(1);
            return;
        }
        // we want to unpause when we're no longer primary
        // start() also loads _lastOpTimeFetched, which we know is set from the "if"
        else if (_pause) {
            start();
        }

        produce();
    }

    void BackgroundSync::produce() {
        // this oplog reader does not do a handshake because we don't want the server it's syncing
        // from to track how far it has synced
        OplogReader r(false /* doHandshake */);

        // find a target to sync from the last op time written
        getOplogReader(r);

        // no server found
        {
            boost::unique_lock<boost::mutex> lock(_mutex);

            if (_currentSyncTarget == NULL) {
                lock.unlock();
                sleepsecs(1);
                // if there is no one to sync from
                return;
            }
            ::abort();
            r.tailingQueryGTE(rsoplog, _lastGTIDFetched);
        }

        // if target cut connections between connecting and querying (for
        // example, because it stepped down) we might not have a cursor
        if (!r.haveCursor()) {
            return;
        }

        if (isRollbackRequired(r)) {
            stop();
            return;
        }

        while (!inShutdown()) {
            while (!inShutdown()) {
                if (!r.moreInCurrentBatch()) {
                    if (theReplSet->gotForceSync()) {
                        return;
                    }

                    if (theReplSet->isPrimary()) {
                        return;
                    }

                    {
                        boost::unique_lock<boost::mutex> lock(_mutex);
                        if (!_currentSyncTarget || !_currentSyncTarget->hbinfo().hbstate.readable()) {
                            return;
                        }
                    }

                    r.more();
                }

                if (!r.more())
                    break;

                BSONObj o = r.nextSafe().getOwned();

                Timer timer;
                // the blocking queue will wait (forever) until there's room for us to push
                OCCASIONALLY {
                    LOG(2) << "bgsync buffer has " << _buffer.size() << " bytes" << rsLog;
                }
                _buffer.push(o);

                {
                    boost::unique_lock<boost::mutex> lock(_mutex);

                    // update counters
                    _queueCounter.waitTime += timer.millis();
                    _queueCounter.numElems++;
                    _lastOpTimeFetched = o["ts"]._opTime();
                }
            } // end while

            {
                boost::unique_lock<boost::mutex> lock(_mutex);
                if (_pause || !_currentSyncTarget || !_currentSyncTarget->hbinfo().hbstate.readable()) {
                    return;
                }
            }


            r.tailCheck();
            if( !r.haveCursor() ) {
                LOG(1) << "replSet end syncTail pass" << rsLog;
                return;
            }

            // looping back is ok because this is a tailable cursor
        }
    }

    bool BackgroundSync::peek(BSONObj* op) {
        return _buffer.peek(*op);
    }

    void BackgroundSync::waitForMore() {
        BSONObj op;
        // Block for one second before timing out.
        // Ignore the value of the op we peeked at.
        _buffer.blockingPeek(op, 1);
    }

    void BackgroundSync::consume() {
        // this is just to get the op off the queue, it's been peeked at 
        // and queued for application already
        _buffer.blockingPop();

        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            _queueCounter.numElems--;
        }
    }

    bool BackgroundSync::isStale(OplogReader& r, BSONObj& remoteOldestOp) {
        remoteOldestOp = r.findOne(rsoplog, Query());
        OpTime remoteTs = remoteOldestOp["ts"]._opTime();
        DEV {
            log() << "replSet remoteOldestOp:    " << remoteTs.toStringLong() << rsLog;
            log() << "replSet lastOpTimeFetched: " << _lastOpTimeFetched.toStringLong() << rsLog;
        }
        LOG(3) << "replSet remoteOldestOp: " << remoteTs.toStringLong() << rsLog;

        {
            boost::unique_lock<boost::mutex> lock(_mutex);

            if (_lastOpTimeFetched >= remoteTs) {
                return false;
            }
        }

        return true;
    }

    void BackgroundSync::getOplogReader(OplogReader& r) {
        Member *target = NULL, *stale = NULL;
        BSONObj oldest;

        // then we're initial syncing and we're still waiting for this to be set
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            if (_lastOpTimeFetched.isNull()) {
                _currentSyncTarget = NULL;
                return;
            }
        }

        verify(r.conn() == NULL);

        while ((target = theReplSet->getMemberToSyncTo()) != NULL) {
            string current = target->fullName();

            if (!r.connect(current)) {
                log(2) << "replSet can't connect to " << current << " to read operations" << rsLog;
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
            theReplSet->goStale(stale, oldest);
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
            try {
                BSONObj theirLastOp = r.getLastOp(rsoplog);
                if (theirLastOp.isEmpty()) {
                    log() << "replSet error empty query result from " << hn << " oplog" << rsLog;
                    sleepsecs(2);
                    return true;
                }
                OpTime theirTS = theirLastOp["ts"]._opTime();
                if (theirTS < _lastOpTimeFetched) {
                    log() << "replSet we are ahead of the primary, will try to roll back" << rsLog;
                    ::abort();
                    //theReplSet->syncRollback(r);
                    return true;
                }
                /* we're not ahead?  maybe our new query got fresher data.  best to come back and try again */
                log() << "replSet syncTail condition 1" << rsLog;
                sleepsecs(1);
            }
            catch(DBException& e) {
                log() << "replSet error querying " << hn << ' ' << e.toString() << rsLog;
                sleepsecs(2);
            }
            return true;
        }

        ::abort();
        // aborting because we used to have a reference to _lastH here
        /*
        BSONObj o = r.nextSafe();
        OpTime ts = o["ts"]._opTime();
        long long h = o["h"].numberLong();
        if( ts != _lastOpTimeFetched || h != _lastH ) {
            log() << "replSet our last op time fetched: " << _lastOpTimeFetched.toStringPretty() << rsLog;
            log() << "replset source's GTE: " << ts.toStringPretty() << rsLog;
            ::abort();
            //theReplSet->syncRollback(r);
            return true;
        }
        */

        return false;
    }

    Member* BackgroundSync::getSyncTarget() {
        boost::unique_lock<boost::mutex> lock(_mutex);
        return _currentSyncTarget;
    }

    void BackgroundSync::stop() {
        {
            boost::unique_lock<boost::mutex> lock(_mutex);

            _pause = true;
            _currentSyncTarget = NULL;
            _lastOpTimeFetched = OpTime(0,0);
            _queueCounter.numElems = 0;
        }

        if (!_buffer.empty()) {
            log() << "replset " << _buffer.size() << " ops were not applied from buffer, this should "
                  << "cause a rollback on the former primary" << rsLog;
        }

        // get rid of pending ops
        _buffer.clear();
    }

    void BackgroundSync::start() {
        massert(16235, "going to start syncing, but buffer is not empty", _buffer.empty());

        boost::unique_lock<boost::mutex> lock(_mutex);
        _pause = false;

        // reset _last fields with current data
        _lastOpTimeFetched = theReplSet->lastOpTimeWritten;

        LOG(1) << "replset bgsync fetch queue set to: " << _lastOpTimeFetched << " " << rsLog;
   }
} // namespace mongo
