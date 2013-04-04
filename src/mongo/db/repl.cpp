// @file repl.cpp

/**
*    Copyright (C) 2008 10gen Inc.
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

/* Collections we use:

   local.sources         - indicates what sources we pull from as a "slave", and the last update of each
   local.oplog.$main     - our op log as "master"
   local.dbinfo.<dbname> - no longer used???
   local.pair.startup    - [deprecated] can contain a special value indicating for a pair that we have the master copy.
                           used when replacing other half of the pair which has permanently failed.
   local.pair.sync       - [deprecated] { initialsynccomplete: 1 }
*/

#include "pch.h"

#include <boost/thread/thread.hpp>

#include "jsobj.h"
#include "../util/goodies.h"
#include "repl.h"
#include "../util/net/message.h"
#include "../util/background.h"
#include "../client/connpool.h"
#include "db.h"
#include "commands.h"
#include "security.h"
#include "cmdline.h"
#include "repl_block.h"
#include "repl/rs.h"
#include "replutil.h"
#include "repl/connections.h"
#include "ops/update.h"
#include "pcrecpp.h"
#include "mongo/db/instance.h"
#include "mongo/db/queryutil.h"

namespace mongo {

    // our config from command line etc.
    ReplSettings replSettings;

    /* if 1 sync() is running */
    volatile int syncing = 0;
    static volatile int relinquishSyncingSome = 0;

    /* "dead" means something really bad happened like replication falling completely out of sync.
       when non-null, we are dead and the string is informational
    */
    const char *replAllDead = 0;

    time_t lastForcedResync = 0;

} // namespace mongo

namespace mongo {

    /* output by the web console */
    const char *replInfo = "";
    struct ReplInfo {
        ReplInfo(const char *msg) {
            replInfo = msg;
        }
        ~ReplInfo() {
            replInfo = "?";
        }
    };

    bool anyReplEnabled() {
        return theReplSet;
    }

    bool replAuthenticate(DBClientBase *conn);

    void appendReplicationInfo( BSONObjBuilder& result , bool authed , int level ) {

        if ( replSet ) {
            if( theReplSet == 0 || theReplSet->state().shunned() ) {
                result.append("ismaster", false);
                result.append("secondary", false);
                result.append("info", ReplSet::startupStatusMsg.get());
                result.append( "isreplicaset" , true );
                return;
            }

            theReplSet->fillIsMaster(result);
            return;
        }

        if ( replAllDead ) {
            result.append("ismaster", 0);
            string s = string("dead: ") + replAllDead;
            result.append("info", s);
        }
        else {
            result.appendBool("ismaster", _isMaster() );
        }

        if ( level && replSet ) {
            result.append( "info" , "is replica set" );
        }
        else if ( level ) {
            BSONObjBuilder sources( result.subarrayStart( "sources" ) );

            int n = 0;
            list<BSONObj> src;
            {
                Client::ReadContext ctx( "local.sources", dbpath, authed );
                shared_ptr<Cursor> c = Helpers::findTableScan("local.sources", BSONObj());
                while ( c->ok() ) {
                    src.push_back(c->current());
                    c->advance();
                }
            }

            for( list<BSONObj>::const_iterator i = src.begin(); i != src.end(); i++ ) {
                BSONObj s = *i;
                BSONObjBuilder bb;
                bb.append( s["host"] );
                string sourcename = s["source"].valuestr();
                if ( sourcename != "main" )
                    bb.append( s["source"] );
                {
                    BSONElement e = s["syncedTo"];
                    BSONObjBuilder t( bb.subobjStart( "syncedTo" ) );
                    t.appendDate( "time" , e.timestampTime() );
                    t.append( "inc" , e.timestampInc() );
                    t.done();
                }

                if ( level > 1 ) {
                    wassert( !Lock::isLocked() );
                    // note: there is no so-style timeout on this connection; perhaps we should have one.
                    scoped_ptr<ScopedDbConnection> conn(
                            ScopedDbConnection::getInternalScopedDbConnection(
                                    s["host"].valuestr() ) );
                    DBClientConnection *cliConn = dynamic_cast< DBClientConnection* >( &conn->conn() );
                    if ( cliConn && replAuthenticate( cliConn ) ) {
                        BSONObj first = conn->get()->findOne( (string)"local.oplog.$" + sourcename,
                                                              Query().sort( BSON( "$natural" << 1 ) ) );
                        BSONObj last = conn->get()->findOne( (string)"local.oplog.$" + sourcename,
                                                             Query().sort( BSON( "$natural" << -1 ) ) );
                        bb.appendDate( "masterFirst" , first["ts"].timestampTime() );
                        bb.appendDate( "masterLast" , last["ts"].timestampTime() );
                        double lag = (double) (last["ts"].timestampTime() - s["syncedTo"].timestampTime());
                        bb.append( "lagSeconds" , lag / 1000 );
                    }
                    conn->done();
                }

                sources.append( BSONObjBuilder::numStr( n++ ) , bb.obj() );
            }

            sources.done();
        }
    }

    class CmdIsMaster : public Command {
    public:
        virtual bool requiresAuth() { return false; }
        virtual bool slaveOk() const {
            return true;
        }
        virtual void help( stringstream &help ) const {
            help << "Check if this server is primary for a replica pair/set; also if it is --master or --slave in simple master/slave setups.\n";
            help << "{ isMaster : 1 }";
        }
        virtual LockType locktype() const { return NONE; }
        CmdIsMaster() : Command("isMaster", true, "ismaster") { }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool /*fromRepl*/) {
            /* currently request to arbiter is (somewhat arbitrarily) an ismaster request that is not
               authenticated.
               we allow unauthenticated ismaster but we aren't as verbose informationally if
               one is not authenticated for admin db to be safe.
            */
            bool authed = cc().getAuthenticationInfo()->isAuthorizedReads("admin");
            appendReplicationInfo( result , authed );

            result.appendNumber("maxBsonObjectSize", BSONObjMaxUserSize);
            result.appendDate("localTime", jsTime());
            return true;
        }
    } cmdismaster;

    BSONObj opTimeQuery = fromjson("{\"getoptime\":1}");    
    DatabaseIgnorer ___databaseIgnorer;
    
    void DatabaseIgnorer::doIgnoreUntilAfter( const string &db, const OpTime &futureOplogTime ) {
        if ( futureOplogTime > _ignores[ db ] ) {
            _ignores[ db ] = futureOplogTime;   
        }
    }

    bool DatabaseIgnorer::ignoreAt( const string &db, const OpTime &currentOplogTime ) {
        if ( _ignores[ db ].isNull() ) {
            return false;
        }
        if ( _ignores[ db ] >= currentOplogTime ) {
            return true;
        } else {
            // The ignore state has expired, so clear it.
            _ignores.erase( db );
            return false;
        }
    }

    extern unsigned replApplyBatchSize;

    BSONObj userReplQuery = fromjson("{\"user\":\"repl\"}");

    bool replAuthenticate(DBClientBase *conn) {
        if( noauth ) {
            return true;
        }
        if( ! cc().isAdmin() ) {
            log() << "replauthenticate: requires admin permissions, failing\n";
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
                Lock::GlobalWrite lk;
                Client::Context ctxt("local.");
                if( !Helpers::findOne("local.system.users", userReplQuery, user) ||
                        // try the first user in local
                        !Helpers::getSingleton("local.system.users", user) ) {
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
        if ( internalSecurity.pwd.length() > 0 ) {
            conn->setAuthenticationTable(
                    AuthenticationTable::getInternalSecurityAuthenticationTable() );
        }
        return true;
    }

    bool replHandshake(DBClientConnection *conn) {
        string myname = getHostName();

        BSONObj me;
        {
            
            Lock::DBWrite l("local");
            // local.me is an identifier for a server for getLastError w:2+
            if ( ! Helpers::getSingleton( "local.me" , me ) ||
                 ! me.hasField("host") ||
                 me["host"].String() != myname ) {

                // clean out local.me
                Helpers::emptyCollection("local.me");

                // repopulate
                BSONObjBuilder b;
                b.appendOID( "_id" , 0 , true );
                b.append( "host", myname );
                me = b.obj();
                Helpers::putSingleton( "local.me" , me );
            }
        }

        BSONObjBuilder cmd;
        cmd.appendAs( me["_id"] , "handshake" );
        if (theReplSet) {
            cmd.append("member", theReplSet->selfId());
        }

        BSONObj res;
        bool ok = conn->runCommand( "admin" , cmd.obj() , res );
        // ignoring for now on purpose for older versions
        log(ok) << "replHandshake res not: " << ok << " res: " << res << endl;
        return true;
    }

    OplogReader::OplogReader( bool doHandshake ) : 
        _doHandshake( doHandshake ) { 
        
        _tailingQueryOptions = QueryOption_SlaveOk;
        _tailingQueryOptions |= QueryOption_CursorTailable | QueryOption_OplogReplay;
        
        /* TODO: slaveOk maybe shouldn't use? */
        _tailingQueryOptions |= QueryOption_AwaitData;
    }

    bool OplogReader::commonConnect(const string& hostName) {
        if( conn() == 0 ) {
            _conn = shared_ptr<DBClientConnection>(new DBClientConnection( false, 0, 60*10 /* tcp timeout */));
            string errmsg;
            ReplInfo r("trying to connect to sync source");
            if ( !_conn->connect(hostName.c_str(), errmsg) ||
                 (!noauth && !replAuthenticate(_conn.get())) ) {
                resetConnection();
                log() << "repl: " << errmsg << endl;
                return false;
            }
        }
        return true;
    }
    
    bool OplogReader::connect(string hostName) {
        if (conn() != 0) {
            return true;
        }

        if ( ! commonConnect(hostName) ) {
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
        if (commonConnect(to)) {
            log() << "handshake between " << from << " and " << to << endl;
            return passthroughHandshake(rid, from);
        }
        return false;
    }

    bool OplogReader::passthroughHandshake(const BSONObj& rid, const int f) {
        BSONObjBuilder cmd;
        cmd.appendAs( rid["_id"], "handshake" );
        cmd.append( "member" , f );

        BSONObj res;
        return conn()->runCommand( "admin" , cmd.obj() , res );
    }

    void OplogReader::tailingQuery(const char *ns, const BSONObj& query, const BSONObj* fields ) {
        verify( !haveCursor() );
        LOG(2) << "repl: " << ns << ".find(" << query.toString() << ')' << endl;
        cursor.reset( _conn->query( ns, query, 0, 0, fields, _tailingQueryOptions ).release() );
    }
    
    void OplogReader::tailingQueryGTE(const char *ns, OpTime t, const BSONObj* fields ) {
        BSONObjBuilder q;
        q.appendDate("$gte", t.asDate());
        BSONObjBuilder query;
        query.append("ts", q.done());
        tailingQuery(ns, query.done(), fields);
    }


    void newRepl();
    void startReplSets(ReplSetCmdline*);
    void startReplication() {
        /* if we are going to be a replica set, we aren't doing other forms of replication. */
        if( !cmdLine._replSet.empty() ) {
            newRepl();

            replSet = true;
            setTxnLogOperations(true);
            setLogTxnToOplog(logTransactionOps);
            ReplSetCmdline *replSetCmdline = new ReplSetCmdline(cmdLine._replSet);
            boost::thread t( boost::bind( &startReplSets, replSetCmdline) );

            return;
        }
        // we should only be running with replica sets
        // we do not support the old master/slave replication
        else {
            return;
        }
    }

    class ReplApplyBatchSizeValidator : public ParameterValidator {
    public:
        ReplApplyBatchSizeValidator() : ParameterValidator( "replApplyBatchSize" ) {}

        virtual bool isValid( BSONElement e , string& errmsg ) const {
            return false;
        }
    } replApplyBatchSizeValidator;
    
    /** we allow queries to SimpleSlave's */
    void replVerifyReadsOk(const ParsedQuery* pq) {
        if( replSet ) {
            // todo: speed up the secondary case.  as written here there are 2 mutex entries, it
            // can b 1.
            if( isMaster() ) return;
            uassert(13435, "not master and slaveOk=false",
                    !pq || pq->hasOption(QueryOption_SlaveOk) || pq->hasReadPref());
            uassert(13436,
                    "not master or secondary; cannot currently read from this replSet member",
                    theReplSet && theReplSet->isSecondary() );
        }
    }
} // namespace mongo
