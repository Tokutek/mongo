/** @file oplogreader.cpp */
#include "pch.h"

#include "pcrecpp.h"
#include <boost/thread/thread.hpp>

#include "mongo/base/counter.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/repl.h"
#include "mongo/util/net/message.h"
#include "mongo/util/background.h"
#include "mongo/client/connpool.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/repl/connections.h"
#include "mongo/db/instance.h"
#include "mongo/db/collection.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/relock.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/update.h"

namespace mongo {
    const BSONObj reverseIDObj = BSON( "_id" << -1 );

    BSONObj userReplQuery = fromjson("{\"user\":\"repl\"}");

    /* Generally replAuthenticate will only be called within system threads to fully authenticate
     * connections to other nodes in the cluster that will be used as part of internal operations.
     * If a user-initiated action results in needing to call replAuthenticate, you can call it
     * with skipAuthCheck set to false. Only do this if you are certain that the proper auth
     * checks have already run to ensure that the user is authorized to do everything that this
     * connection will be used for!
     */
    bool replAuthenticate(DBClientBase *conn, bool skipAuthCheck) {
        if(!AuthorizationManager::isAuthEnabled()) {
            return true;
        }
        if (!skipAuthCheck && !cc().getAuthorizationManager()->hasInternalAuthorization()) {
            log() << "replauthenticate: requires internal authorization, failing" << endl;
            return false;
        }

        string u;
        string p;
        if (internalSecurity.pwd.length() > 0) {
            u = internalSecurity.user;
            p = internalSecurity.pwd;
        }
        else {
            BSONObj user;
            {
                StringData ns("local.system.users");
                LOCK_REASON(lockReason, "repl: authenticating with local db");
                Client::ReadContext ctx(ns, lockReason);
                if (!Collection::findOne(ns, userReplQuery, user) ||
                        // try the first user in local
                        !Collection::findOne(ns, BSONObj(), user)) {
                    log() << "replauthenticate: no user in local.system.users to use for authentication\n";
                    return false;
                }
            }
            u = user.getStringField("user");
            p = user.getStringField("pwd");
            massert( 10392 , "bad user object? [1]", !u.empty());
            massert( 10393 , "bad user object? [2]", !p.empty());
        }

        string err;
        if( !conn->auth("local", u.c_str(), p.c_str(), err, false) ) {
            log() << "replauthenticate: can't authenticate to master server, user:" << u << endl;
            return false;
        }

        return true;
    }

    void getMe(BSONObj& me) {
        const string myname = getHostName();
        Client::Transaction transaction(0);            

        // local.me is an identifier for a server for getLastError w:2+
        if (!Collection::findOne("local.me", BSONObj(), me) ||
                !me.hasField("host") || me["host"].String() != myname) {
        
            // cleaning out local.me requires write
            // lock. This is a rare operation, so it should
            // be ok
            if (!Lock::isWriteLocked("local")) {
                throw RetryWithWriteLock();
            }

            // clean out local.me
            deleteObjects("local.me", BSONObj(), false, false);
        
            // repopulate
            BSONObjBuilder b;
            b.appendOID( "_id" , 0 , true );
            b.append( "host", myname );
            me = b.obj();
            updateObjects("local.me", me, BSONObj(), true, false);
        }
        transaction.commit(0);
    }

    bool replHandshake(DBClientConnection *conn) {
        BSONObj me;
        LOCK_REASON(lockReason, "repl: handshake");
        try {
            Client::ReadContext ctx("local", lockReason);
            getMe(me);
        }
        catch (RetryWithWriteLock &e) {
            Client::WriteContext ctx("local", lockReason);
            getMe(me);
        }

        BSONObjBuilder cmd;
        cmd.appendAs( me["_id"] , "handshake" );
        if (theReplSet) {
            cmd.append("member", theReplSet->selfId());
        }

        BSONObj res;
        bool ok = conn->runCommand( "admin" , cmd.obj() , res );
        // ignoring for now on purpose for older versions
        LOG(ok ? 1 : 0) << "replHandshake res not: " << ok << " res: " << res << endl;
        return true;
    }

    //number of readers created;
    //  this happens when the source source changes, a reconfig/network-error or the cursor dies
    static Counter64 readersCreatedStats;
    static ServerStatusMetricField<Counter64> displayReadersCreated(
                                                    "repl.network.readersCreated",
                                                    &readersCreatedStats );

    OplogReader::OplogReader( bool doHandshake ) : 
        _doHandshake( doHandshake ) { 
        
        _tailingQueryOptions = QueryOption_SlaveOk;
        _tailingQueryOptions |= QueryOption_CursorTailable | QueryOption_OplogReplay;
        
        /* TODO: slaveOk maybe shouldn't use? */
        _tailingQueryOptions |= QueryOption_AwaitData;

        readersCreatedStats.increment();
    }

    bool OplogReader::commonConnect(const string& hostName, const double default_timeout) {
        if( conn() == 0 ) {
            _conn = shared_ptr<DBClientConnection>(new DBClientConnection(false,
                                                                          0,
                                                                          default_timeout /* tcp timeout */));
            string errmsg;
            if ( !_conn->connect(hostName.c_str(), errmsg) ||
                 (AuthorizationManager::isAuthEnabled() && !replAuthenticate(_conn.get(), true)) ) {
                resetConnection();
                log() << "repl: " << errmsg << endl;
                return false;
            }
        }
        return true;
    }
    
    bool OplogReader::connect(const std::string& hostName, const double default_timeout) {
        if (conn() != 0) {
            return true;
        }

        if ( ! commonConnect(hostName, default_timeout) ) {
            return false;
        }
        
        
        if ( _doHandshake && ! replHandshake(_conn.get() ) ) {
            return false;
        }

        return true;
    }

    bool OplogReader::connect(const BSONObj& rid, const int from, const string& to) {
        if (conn() != 0) {
            return true;
        }
        if (commonConnect(to, default_so_timeout)) {
            log() << "handshake between " << from << " and " << to << endl;
            return passthroughHandshake(rid, from);
        }
        return false;
    }

    bool OplogReader::passthroughHandshake(const BSONObj& rid, const int nextOnChainId) {
        BSONObjBuilder cmd;
        cmd.appendAs(rid["_id"], "handshake");
        if (theReplSet) {
            const Member* chainedMember = theReplSet->findById(nextOnChainId);
            if (chainedMember != NULL) {
                cmd.append("config", chainedMember->config().asBson());
            }
        }
        cmd.append("member", nextOnChainId);

        BSONObj res;
        return conn()->runCommand("admin", cmd.obj(), res);
    }

    void OplogReader::tailingQuery(const char *ns, Query& query, const BSONObj* fields ) {
        verify( !haveCursor() );
        LOG(2) << "repl: " << ns << ".find(" << query.toString() << ')' << endl;
        cursor.reset( _conn->query( ns, query, 0, 0, fields, _tailingQueryOptions ).release() );
    }
    
    void OplogReader::tailingQueryGTE(const char *ns, GTID gtid, const BSONObj* fields ) {
        BSONObjBuilder q;
        addGTIDToBSON("$gte", gtid, q);
        BSONObjBuilder query;
        query.append("_id", q.done());
        tailingQuery(ns, Query(query.done()).hint(BSON("_id" << 1)), fields);
    }

    shared_ptr<DBClientCursor> OplogReader::getRollbackCursor(GTID lastGTID) {
        shared_ptr<DBClientCursor> retCursor;
        BSONObjBuilder q;
        addGTIDToBSON("$lte", lastGTID, q);
        BSONObjBuilder query;
        query.append("_id", q.done());
        retCursor.reset(
            _conn->query(rsoplog, Query(query.done()).sort(reverseIDObj), 0, 0, NULL, QueryOption_SlaveOk).release()
            );
        return retCursor;
    }

    bool OplogReader::propogateSlaveLocation(GTID lastGTID){
        BSONObjBuilder cmd;
        cmd.append("updateSlave", 1);
        addGTIDToBSON("gtid", lastGTID, cmd);
        BSONObj ret;
        return _conn->runCommand(
            "local",
            cmd.done(),
            ret
            );
    }

    shared_ptr<DBClientCursor> OplogReader::getOplogRefsCursor(OID &oid) {
        shared_ptr<DBClientCursor> retCursor;
        // this maps to {_id : {$gt : { oid : oid , seq : 0 }}}
        retCursor.reset(_conn->query(rsOplogRefs, QUERY("_id" << BSON("$gt" << BSON("oid" << oid << "seq" << 0)) ).hint(BSON("_id" << 1))).release());
        return retCursor;
    }
}
