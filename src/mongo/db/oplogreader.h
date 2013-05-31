/** @file oplogreader.h */

#pragma once

#include "../client/constants.h"
#include "dbhelpers.h"
#include "mongo/client/dbclientcursor.h"

namespace mongo {

    /* started abstracting out the querying of the primary/master's oplog
       still fairly awkward but a start.
    */

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
        BSONObj findOne(const char *ns, const Query& q) {
            return conn()->findOne(ns, q, 0, QueryOption_SlaveOk);
        }
        BSONObj getLastOp(const char *ns) {
            return findOne(ns, Query().sort(reverseNaturalObj));
        }

        /* ok to call if already connected */
        bool connect(string hostname);

        bool connect(const BSONObj& rid, const int from, const string& to);

        void tailCheck() {
            if( cursor.get() && cursor->isDead() ) {
                log() << "repl: old cursor isDead, will initiate a new one" << endl;
                resetCursor();
            }
        }

        bool haveCursor() { return cursor.get() != 0; }

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

        int getTailingQueryOptions() const { return _tailingQueryOptions; }

        void peek(vector<BSONObj>& v, int n) {
            if( cursor.get() ) {
                cursor->peek(v,n);
            }
        }
        BSONObj nextSafe() { return cursor->nextSafe(); }
        BSONObj next() { return cursor->next(); }
        
    private:
        /** @return true iff connection was successful */ 
        bool commonConnect(const string& hostName);
        bool passthroughHandshake(const BSONObj& rid, const int f);
        void tailingQuery(const char *ns, Query& query, const BSONObj* fields=0);
    };
}
