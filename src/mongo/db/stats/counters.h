// counters.h
/*
 *    Copyright (C) 2010 10gen Inc.
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

#pragma once

#include "mongo/pch.h"
#include "mongo/db/jsobj.h"
#include "mongo/util/net/message.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/concurrency/spin_lock.h"

namespace mongo {

    /**
     * for storing operation counters
     * note: not thread safe.  ok with that for speed
     */
    class OpCounters {
    public:

        OpCounters();
        void gotInsert(const int n = 1) { _insert.fetchAndAdd(n); }
        void gotQuery() { _query.fetchAndAdd(1); }
        void gotUpdate() { _update.fetchAndAdd(1); }
        void gotDelete() { _delete.fetchAndAdd(1); }
        void gotGetMore() { _getmore.fetchAndAdd(1); }
        void gotCommand() { _command.fetchAndAdd(1); }

        void gotOp( int op , bool isCommand );

        BSONObj getObj() const;
        
    private:
        void _checkWrap();
        
        // ensures that the next member does not sit on the same cacheline as any real data.
        AtomicWordOnCacheLine _dummyCounter;
        AtomicWordOnCacheLine _insert;
        AtomicWordOnCacheLine _query;
        AtomicWordOnCacheLine _update;
        AtomicWordOnCacheLine _delete;
        AtomicWordOnCacheLine _getmore;
        AtomicWordOnCacheLine _command;
    };

    extern OpCounters globalOpCounters;
    extern OpCounters replOpCounters;

    class NetworkCounter {
    public:
        NetworkCounter() { }
        void hit(const long long bytesIn, const long long bytesOut);
        void append(BSONObjBuilder &b);
    private:
        // ensures that the next member does not sit on the same cacheline as any real data.
        AtomicWordOnCacheLine _dummyCounter;
        AtomicWordOnCacheLine _bytesIn;
        AtomicWordOnCacheLine _bytesOut;
        AtomicWordOnCacheLine _requests;
    };

    extern NetworkCounter networkCounter;
}
