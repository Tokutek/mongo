// counters.cpp
/*
 *    Copyright (C) 2010 10gen Inc.
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


#include "pch.h"
#include "../jsobj.h"
#include "counters.h"

namespace mongo {
    OpCounters::OpCounters() {}

    void OpCounters::gotOp( int op , bool isCommand ) {
        switch ( op ) {
        case dbInsert: /*gotInsert();*/ break; // need to handle multi-insert
        case dbQuery:
            if ( isCommand )
                gotCommand();
            else
                gotQuery();
            break;

        case dbUpdate: gotUpdate(); break;
        case dbDelete: gotDelete(); break;
        case dbGetMore: gotGetMore(); break;
        case dbKillCursors:
        case opReply:
        case dbMsg:
            break;
        default: log() << "OpCounters::gotOp unknown op: " << op << endl;
        }
    }

    BSONObj OpCounters::getObj() {
        const unsigned MAX = 1 << 30;
        RARELY {
            bool wrap =
            _insert.get() > MAX ||
            _query.get() > MAX ||
            _update.get() > MAX ||
            _delete.get() > MAX ||
            _getmore.get() > MAX ||
            _command.get() > MAX;

            if ( wrap ) {
                _insert.zero();
                _query.zero();
                _update.zero();
                _delete.zero();
                _getmore.zero();
                _command.zero();
            }

        }
        BSONObjBuilder b;
        {
            b.append( "insert" , _insert.get() );
            b.append( "query" , _query.get() );
            b.append( "update" , _update.get() );
            b.append( "delete" , _delete.get() );
            b.append( "getmore" , _getmore.get() );
            b.append( "command" , _command.get() );
        }
        return b.obj();
    }

    void GenericCounter::hit( const string& name , int count ) {
        scoped_lock lk( _mutex );
        _counts[name]++;
    }

    BSONObj GenericCounter::getObj() {
        BSONObjBuilder b(128);
        {
            mongo::mutex::scoped_lock lk( _mutex );
            for ( map<string,long long>::iterator i=_counts.begin(); i!=_counts.end(); i++ ) {
                b.appendNumber( i->first , i->second );
            }
        }
        return b.obj();
    }


    void NetworkCounter::hit( long long bytesIn , long long bytesOut ) {
        const long long MAX = 1ULL << 60;

        // don't care about the race as its just a counter
        bool overflow = _bytesIn > MAX || _bytesOut > MAX;

        if ( overflow ) {
            _lock.lock();
            _overflows++;
            _bytesIn = bytesIn;
            _bytesOut = bytesOut;
            _requests = 1;
            _lock.unlock();
        }
        else {
            _lock.lock();
            _bytesIn += bytesIn;
            _bytesOut += bytesOut;
            _requests++;
            _lock.unlock();
        }
    }

    void NetworkCounter::append( BSONObjBuilder& b ) {
        _lock.lock();
        b.appendNumber( "bytesIn" , _bytesIn );
        b.appendNumber( "bytesOut" , _bytesOut );
        b.appendNumber( "numRequests" , _requests );
        _lock.unlock();
    }

    OpCounters globalOpCounters;
    OpCounters replOpCounters;
    NetworkCounter networkCounter;
}
