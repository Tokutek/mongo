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
