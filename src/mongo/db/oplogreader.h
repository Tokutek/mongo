/** @file oplogreader.h */

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

#include "mongo/client/constants.h"
#include "mongo/client/dbclientcursor.h"

namespace mongo {

    extern const BSONObj reverseIDObj;

    /* started abstracting out the querying of the primary/master's oplog
       still fairly awkward but a start.
    */
    const double default_so_timeout = 30;

    class OplogReader {
        shared_ptr<DBClientConnection> _conn;
        shared_ptr<DBClientCursor> cursor;
        bool _doHandshake;
        int _tailingQueryOptions;
    public:
        OplogReader( bool doHandshake = true );
        ~OplogReader() { }
        void resetCursor() { cursor.reset(); }
        void resetConnection() {
            cursor.reset();
            _conn.reset();
        }
        shared_ptr<DBClientConnection> conn_shared() { return _conn; }
        DBClientConnection* conn() { return _conn.get(); }
        BSONObj getLastOp(const char *ns) {
            return conn()->findOne(ns, Query().sort(reverseIDObj), 0, QueryOption_SlaveOk);
        }

        /* ok to call if already connected */
        bool connect(const std::string& hostname, const double default_timeout = default_so_timeout);

        bool connect(const BSONObj& rid, const int from, const string& to);

        void tailCheck() {
            if( cursor.get() && cursor->isDead() ) {
                log() << "repl: old cursor isDead, will initiate a new one" << endl;
                resetCursor();
            }
        }

        bool haveCursor() { return cursor.get() != 0; }
        bool haveConnection() { return _conn.get() != 0; }

        bool propogateSlaveLocation(GTID lastGTID);

        void tailingQueryGTE(const char *ns, GTID gtid, const BSONObj* fields=0);

        /* Do a tailing query, but only send the ts field back. */
        void ghostQueryGTE(const char *ns, GTID gtid) {
            const BSONObj fields = BSON("ts" << 1 << "_id" << 1);
            return tailingQueryGTE(ns, gtid, &fields);
        }

        // gets a cursor on the remote oplog that runs in reverse, starting from
        // the greatest GTID that is less than or equal to lastGTID
        shared_ptr<DBClientCursor> getRollbackCursor(GTID lastGTID);
        

        bool more() {
            uassert( 15910, "Doesn't have cursor for reading oplog", cursor.get() );
            return cursor->more();
        }

        bool moreInCurrentBatch() {
            uassert( 15911, "Doesn't have cursor for reading oplog", cursor.get() );
            return cursor->moreInCurrentBatch();
        }

        int currentBatchMessageSize() {
            if( NULL == cursor->getMessage() )
                return 0;
            return cursor->getMessage()->size();
        }

        int getTailingQueryOptions() const { return _tailingQueryOptions; }

        void peek(vector<BSONObj>& v, int n) {
            if( cursor.get() ) {
                cursor->peek(v,n);
            }
        }
        BSONObj nextSafe() { return cursor->nextSafe(); }
        BSONObj next() { return cursor->next(); }

        shared_ptr<DBClientCursor> getOplogRefsCursor(OID &oid);

    private:
        /** @return true iff connection was successful */ 
        bool commonConnect(const string& hostName, const double default_timeout);
        bool passthroughHandshake(const BSONObj& rid, const int f);
        void tailingQuery(const char *ns, Query& query, const BSONObj* fields=0);
    };
}
