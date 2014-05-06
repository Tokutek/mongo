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
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
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
