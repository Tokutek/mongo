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

#pragma once

#include <boost/thread/mutex.hpp>

#include "mongo/util/queue.h"
#include "mongo/db/oplogreader.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/jsobj.h"

namespace mongo {


    /**
     * Lock order:
     * 1. rslock
     * 2. rwlock
     * 3. BackgroundSync::_mutex
     */
    class BackgroundSync {
        static BackgroundSync *s_instance;
        // protects creation of s_instance
        static boost::mutex s_mutex;

        // _mutex protects all of the class variables
        boost::mutex _mutex;
        // condition variable to signal changes in whether
        // opsync thread  is running.
        boost::condition _opSyncRunningCondVar;
        // condition variable to signal changes in permission
        // of opsync thread running.
        boost::condition _opSyncCanRunCondVar;

        // signals events related to the elements in the queue
        boost::condition _queueCond;

        // signals when the applier has nothing to do
        boost::condition _queueDone;

        // boolean that states whether we should actively be 
        // trying to read data from another machine and apply it
        // to our opLog. When we are a secondary, this should be true.
        // When we are a primary, this should be false.
        bool _opSyncShouldRun;
        // boolean that states if we are in the process of running the opSync
        // thread.
        bool _opSyncRunning;

        GTID _lastGTIDFetched;

        Member* _currentSyncTarget;


        struct QueueCounter {
            QueueCounter();
            unsigned long long waitTime;
            unsigned int numElems;
        } _queueCounter;

        BackgroundSync();
        BackgroundSync(const BackgroundSync& s);
        BackgroundSync operator=(const BackgroundSync& s);


        // Production thread
        uint32_t produce();
        // Check if rollback is necessary
        bool isRollbackRequired(OplogReader& r);
        void getOplogReader(OplogReader& r);
        // check latest GTID against the remote's earliest GTID, filling in remoteOldestOp.
        bool isStale(OplogReader& r, BSONObj& remoteOldestOp);

        bool hasCursor();
    public:
        static BackgroundSync* get();
        static void shutdown();
        virtual ~BackgroundSync() {}

        void applierThread();
        void applyOpsFromOplog();

        // starts the producer thread
        void producerThread();

        virtual Member* getSyncTarget();

        // For monitoring
        BSONObj getCounters();

        // for when we are assuming a primary
        void stopOpSyncThread();

        // for when we become a secondary
        void startOpSyncThread();

    };


} // namespace mongo
