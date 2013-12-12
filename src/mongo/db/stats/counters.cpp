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

    BSONObj OpCounters::getObj() const {
        BSONObjBuilder b;
        b.append("insert", _insert.loadRelaxed());
        b.append("query", _query.loadRelaxed());
        b.append("update", _update.loadRelaxed());
        b.append("delete", _delete.loadRelaxed());
        b.append("getmore", _getmore.loadRelaxed());
        b.append("command", _command.loadRelaxed());
        return b.obj();
    }

    void NetworkCounter::hit(const long long bytesIn, const long long bytesOut) {
        _bytesIn.fetchAndAdd(bytesIn);
        _bytesOut.fetchAndAdd(bytesOut);
        _requests.fetchAndAdd(1);
    }

    void NetworkCounter::append(BSONObjBuilder &b) {
        b.appendNumber("bytesIn", _bytesIn.loadRelaxed());
        b.appendNumber("bytesOut" , _bytesOut.loadRelaxed());
        b.appendNumber("numRequests", _requests.loadRelaxed());
    }

    OpCounters globalOpCounters;
    OpCounters replOpCounters;
    NetworkCounter networkCounter;
}
