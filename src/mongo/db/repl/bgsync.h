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

        // Production thread
        BlockingQueue<BSONObj> _buffer;

        GTID _lastGTIDFetched;
        // if produce thread should be running
        bool _pause;

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
        void _producerThread();
        void produce();
        // Check if rollback is necessary
        bool isRollbackRequired(OplogReader& r);
        void getOplogReader(OplogReader& r);
        // check latest GTID against the remote's earliest GTID, filling in remoteOldestOp.
        bool isStale(OplogReader& r, BSONObj& remoteOldestOp);
        // stop syncing when this becomes a primary
        void stop();
        // restart syncing
        void start();

        bool hasCursor();
    public:
        static BackgroundSync* get();
        static void shutdown();
        virtual ~BackgroundSync() {}

        // starts the producer thread
        void producerThread();

        // Interface implementation

        virtual bool peek(BSONObj* op);
        virtual void consume();
        virtual Member* getSyncTarget();
        virtual void waitForMore();

        // For monitoring
        BSONObj getCounters();
    };


} // namespace mongo
