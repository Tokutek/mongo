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
                                       _pause(true),
                                       _currentSyncTarget(NULL)
    {
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

        if (_pause) {
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

                if (!r.more()) {
                    break;
                }

                // This is the operation we have received from the target
                // that we must put in our oplog with an applied field of false
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
        GTID remoteOldestGTID = getGTIDFromBSON("_id", remoteOldestOp);
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            GTID currLiveState = theReplSet->gtidManager->getLiveState();
            if (GTID::cmp(currLiveState, remoteOldestGTID) <= 0) {
                return true;
            }
        }
        return false;
    }

    void BackgroundSync::getOplogReader(OplogReader& r) {
        Member *target = NULL, *stale = NULL;
        BSONObj oldest;

        // TODO: What if we're initial syncing and we're still waiting for this to be set
        /*
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            if (_lastOpTimeFetched.isNull()) {
                _currentSyncTarget = NULL;
                return;
            }
        }
        */

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
        // TODO: reimplement this
        ::abort();
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
            _queueCounter.numElems = 0;
        }
    }

    void BackgroundSync::start() {
        boost::unique_lock<boost::mutex> lock(_mutex);
        _pause = false;
   }
} // namespace mongo
