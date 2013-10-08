// dbcommands.cpp

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

/* SHARDING: 
   I believe this file is for mongod only.
   See s/commands_public.cpp for mongos.
*/

#include <time.h>

#include "mongo/pch.h"
#include "mongo/server.h"
#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/databaseholder.h"
#include "mongo/db/client.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/introspect.h"
#include "mongo/db/cursor.h"
#include "mongo/db/json.h"
#include "mongo/db/repl.h"
#include "mongo/db/repl_block.h"
#include "mongo/db/replutil.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/rename_collection.h"
#include "mongo/db/instance.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/db/ops/count.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/repl/bgsync.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/s/d_writeback.h"
#include "mongo/scripting/engine.h"
#include "mongo/util/version.h"
#include "mongo/util/lruishmap.h"
#include "mongo/util/md5.hpp"
#include "mongo/util/processinfo.h"
#include "mongo/util/ramlog.h"

namespace mongo {

    /* reset any errors so that getlasterror comes back clean.

       useful before performing a long series of operations where we want to
       see if any of the operations triggered an error, but don't want to check
       after each op as that woudl be a client/server turnaround.
    */
    class CmdResetError : public InformationCommand {
    public:
        CmdResetError() : InformationCommand("resetError", false, "reseterror") {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {} // No auth required
        virtual void help( stringstream& help ) const {
            help << "reset error state (used with getpreverror)";
        }
        bool run(const string& db, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            LastError *le = lastError.get();
            verify( le );
            le->reset();
            return true;
        }
    } cmdResetError;

    /* set by replica sets if specified in the configuration.
       a pointer is used to avoid any possible locking issues with lockless reading (see below locktype() is NONE
       and would like to keep that)
       (for now, it simply orphans any old copy as config changes should be extremely rare).
       note: once non-null, never goes to null again.
    */
    BSONObj *getLastErrorDefault = 0;

    class CmdGetLastError : public InformationCommand {
    public:
        CmdGetLastError() : InformationCommand("getLastError", false, "getlasterror") { }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {} // No auth required
        virtual void help( stringstream& help ) const {
            help << "return error status of the last operation on this connection\n"
                 << "options:\n"
                 << "  { fsync:true } - fsync the recovery log before returning\n"
                 << "  { j:true } - fsync the recovery log before returning\n"
                 << "  { w:n } - replication not supported yet, so does nothing\n"
                 << "  { wtimeout:m} - replication not supported yet, so does nothing";
        }
        bool run(const string& dbname, BSONObj& _cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            LastError *le = lastError.disableForCommand();

            bool err = false;

            if ( le->nPrev != 1 ) {
                err = LastError::noError.appendSelf( result , false );
                le->appendSelfStatus( result );
            }
            else {
                err = le->appendSelf( result , false );
            }

            Client& c = cc();
            c.appendLastGTID( result );

            result.appendNumber( "connectionId" , c.getConnectionId() ); // for sharding; also useful in general for debugging

            BSONObj cmdObj = _cmdObj;
            {
                BSONObj::iterator i(_cmdObj);
                i.next();
                if( !i.more() ) {
                    /* empty, use default */
                    BSONObj *def = getLastErrorDefault;
                    if( def )
                        cmdObj = *def;
                }
            }
            if ( err ) {
                // doesn't make sense to wait for fsync
                // if there was an error
                return true;
            }

            // write concern is only relevant if we are NOT in a multi statement transaction
            // therefore, do nothing if a transaction is live
            if (!cc().hasTxn()) {
                //
                // slight change from MongoDB originally
                // MongoDB allows only j or fsync to be set, not both
                // we allow to set both
                //
                if ( cmdObj["j"].trueValue() || cmdObj["fsync"].trueValue()) {
                    // only bother to flush recovery log 
                    // if we are not already fsyncing on commit
                    if (!cmdLine.logFlushPeriod != 0) {
                        storage::log_flush();
                    }
                }

                BSONElement e = cmdObj["w"];
                if ( e.ok() ) {

                    if ( cmdLine.configsvr && (!e.isNumber() || e.numberInt() > 1) ) {
                        // w:1 on config servers should still work, but anything greater than that
                        // should not.
                        result.append( "wnote", "can't use w on config servers" );
                        result.append( "err", "norepl" );
                        return true;
                    }

                    int timeout = cmdObj["wtimeout"].numberInt();
                    Timer t;

                    long long passes = 0;
                    char buf[32];
                    GTID gtid = c.getLastOp();

                    if ( gtid.isInitial() ) {
                        if ( anyReplEnabled() ) {
                            result.append( "wnote" , "no write has been done on this connection" );
                        }
                        else if ( e.isNumber() && e.numberInt() <= 1 ) {
                            // don't do anything
                            // w=1 and no repl, so this is fine
                        }
                        else {
                            // w=2 and no repl
                            stringstream errmsg;
                            errmsg << "no replication has been enabled, so w=" <<
                                      e.toString(false) << " won't work";
                            result.append( "wnote" , errmsg.str() );
                            result.append( "err", "norepl" );
                            return true;
                        }

                        result.appendNull( "err" );
                        return true;
                    }

                    if ( !theReplSet && !e.isNumber() ) {
                        result.append( "wnote", "cannot use non integer w values for non-replica sets" );
                        result.append( "err", "noreplset" );
                        return true;
                    }

                    while ( 1 ) {
                        if ( !_isMaster() ) {
                            // this should be in the while loop in case we step down
                            errmsg = "not master";
                            result.append( "wnote", "no longer primary" );
                            return false;
                        }

                        // check this first for w=0 or w=1
                        if ( opReplicatedEnough( gtid, e ) ) {
                            // before breaking, let's check that we are not master
                            // originally done outside of if-clause in vanilla by
                            // SERVER-9417. Moved here so that we don't have
                            // race condition machine stepping down after the check
                            // but before the call to opReplicatedEnough
                            if ( !_isMaster() ) {
                                errmsg = "not master";
                                result.append( "wnote", "no longer primary" );
                                return false;
                            }
                            break;
                        }

                        // if replication isn't enabled (e.g., config servers)
                        if ( ! anyReplEnabled() ) {
                            result.append( "err", "norepl" );
                            return true;
                        }

                        if ( timeout > 0 && t.millis() >= timeout ) {
                            result.append( "wtimeout" , true );
                            errmsg = "timed out waiting for slaves";
                            result.append( "waited" , t.millis() );
                            result.append( "err" , "timeout" );
                            return true;
                        }

                        verify( sprintf( buf , "w block pass: %lld" , ++passes ) < 30 );
                        c.curop()->setMessage( buf );
                        sleepmillis(1);
                        killCurrentOp.checkForInterrupt();
                    }

                    int myMillis = t.millis();
                    result.appendNumber( "wtime" , myMillis );
                }
            }

            if (cmdObj["fsync"].trueValue()) {
                // This is part of the protocol used by SyncClusterConnection.
                // We just fake it and say we synced a file to get it to stop complaining.
                result.append( "fsyncFiles" , 1 );
            }

            result.appendNull( "err" );
            return true;
        }
    } cmdGetLastError;

    class CmdGetPrevError : public InformationCommand {
    public:
        CmdGetPrevError() : InformationCommand("getPrevError", false, "getpreverror") {}
        virtual void help( stringstream& help ) const {
            help << "check for errors since last reseterror commandcal";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {} // No auth required
        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            LastError *le = lastError.disableForCommand();
            le->appendSelf( result );
            if ( le->valid )
                result.append( "nPrev", le->nPrev );
            else
                result.append( "nPrev", -1 );
            return true;
        }
    } cmdGetPrevError;

    CmdShutdown cmdShutdown;

    void CmdShutdown::help( stringstream& help ) const {
        help << "shutdown the database.  must be ran against admin db and "
             << "either (1) ran from localhost or (2) authenticated. If "
             << "this is a primary in a replica set and there is no member "
             << "within 10 seconds of its optime, it will not shutdown "
             << "without force : true.  You can also specify timeoutSecs : "
             << "N to wait N seconds for other members to catch up.";
    }

    bool CmdShutdown::run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
        bool force = cmdObj.hasField("force") && cmdObj["force"].trueValue();

        if (!force && theReplSet && theReplSet->isPrimary()) {
            long long timeout, now, start;
            timeout = now = start = curTimeMicros64()/1000000;
            if (cmdObj.hasField("timeoutSecs")) {
                timeout += cmdObj["timeoutSecs"].numberLong();
            }

            uint64_t lastOp = theReplSet->gtidManager->getCurrTimestamp();
            uint64_t closest = theReplSet->lastOtherOpTime();
            GTID lastGTID = theReplSet->gtidManager->getLiveState();
            GTID closestGTID = theReplSet->lastOtherGTID();
            uint64_t diff = (lastOp > closest) ? lastOp - closest : 0;
            while (now <= timeout && diff > 10000) {
                sleepsecs(1);
                now++;

                lastOp = theReplSet->gtidManager->getCurrTimestamp();
                closest = theReplSet->lastOtherOpTime();
                diff = (lastOp > closest) ? lastOp - closest : 0;
            }

            if (diff > 10000) {
                errmsg = "no secondaries within 10 seconds of my optime";
                result.appendNumber("closest", closest/1000);
                result.appendNumber("difference", diff/1000);
                return false;
            }

            // step down
            theReplSet->stepDown(120);

            log() << "waiting for secondaries to catch up" << endl;

            lastGTID = theReplSet->gtidManager->getLiveState();
            while (GTID::cmp(lastGTID, closestGTID) > 0 && now - start < 60) {
                closestGTID = theReplSet->lastOtherGTID();

                now++;
                sleepsecs(1);
            }

            // regardless of whether they caught up, we'll shut down
        }

        if (theReplSet) {
            theReplSet->shutdown();
        }

        writelocktry wlt( 2 * 60 * 1000 );
        uassert( 13455 , "dbexit timed out getting lock" , wlt.got() );
        return shutdownHelper();
    }

    class CmdDropDatabase : public FileopsCommand {
    public:
        CmdDropDatabase() : FileopsCommand("dropDatabase") {}
        virtual bool slaveOk() const { return false; }
        virtual bool logTheOp() { return true; }
        virtual void help( stringstream& help ) const {
            help << "drop (delete) this database";
        }

        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::dropDatabase);
            out->push_back(Privilege(dbname, actions));
        }

        // this is suboptimal but oplogCheckCloseDatabase is called from dropDatabase, and that 
        // may need a global lock.
        virtual bool lockGlobally() const { return true; }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            // disallow dropping the config database
            if ( cmdLine.configsvr && ( dbname == "config" ) ) {
                errmsg = "Cannot drop 'config' database if mongod started with --configsvr";
                return false;
            }
            if ( replSet && (dbname == "local")) {
                errmsg = "Cannot drop 'local' database if mongod started --replSet";
                return false;
            }
            BSONElement e = cmdObj.firstElement();
            log() << "dropDatabase " << dbname << endl;
            int p = (int) e.number();
            if ( p != 1 )
                return false;
            dropDatabase(dbname);
            result.append( "dropped" , dbname );
            return true;
        }
    } cmdDropDatabase;

    /* set db profiling level
       todo: how do we handle profiling information put in the db with replication?
             sensibly or not?
    */
    class CmdProfile : public Command {
    public:
        CmdProfile() : Command("profile", false) {}
        virtual void help( stringstream& help ) const {
            help << "enable or disable performance profiling\n";
            help << "{ profile : <n> }\n";
            help << "0=off 1=log slow ops 2=log all\n";
            help << "-1 to get current values\n";
            help << "http://dochub.mongodb.org/core/databaseprofiler";
        }
        // Need access to the database to enable profiling on it
        virtual bool slaveOk() const { return true; }
        virtual LockType locktype() const { return NONE; }
        virtual bool requiresSync() const { return false; }
        virtual bool needsTxn() const { return false; }
        virtual int txnFlags() const { return 0; }
        virtual bool canRunInMultiStmtTxn() const { return false; }
        virtual OpSettings getOpSettings() const { return OpSettings(); }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::profileEnable);
            out->push_back(Privilege(dbname, actions));
        }

    private:
        bool _run(const string& dbname, BSONObj& cmdObj, int i, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            BSONElement e = cmdObj.firstElement();
            result.append("was", cc().database()->profile());
            result.append("slowms", cmdLine.slowMS );

            int p = (int) e.number();
            bool ok = false;

            if ( p == -1 ) {
                ok = true;
            } else if ( p >= 0 && p <= 2 ) {
                Client::Transaction transaction(DB_SERIALIZABLE);
                ok = cc().database()->setProfilingLevel( p , errmsg );
                transaction.commit();
            }

            BSONElement slow = cmdObj["slowms"];
            if ( slow.isNumber() )
                cmdLine.slowMS = slow.numberInt();

            return ok;
        }

    public:
        bool run(const string& dbname, BSONObj& cmdObj, int i, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            try {
                Client::ReadContext ctx(dbname);
                return _run(dbname, cmdObj, i, errmsg, result, fromRepl);
            } catch (RetryWithWriteLock &e) {
                Client::WriteContext ctx(dbname);
                return _run(dbname, cmdObj, i, errmsg, result, fromRepl);
            }
        }
    } cmdProfile;

    void reportLockStats(BSONObjBuilder& result);
    
    class CmdServerStatus : public WebInformationCommand {
        unsigned long long _started;
    public:
        CmdServerStatus() : WebInformationCommand("serverStatus") {
            _started = curTimeMillis64();
        }

        virtual void help( stringstream& help ) const {
            help << "returns lots of administrative server statistics";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::serverStatus);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            long long start = Listener::getElapsedTimeMillis();
            BSONObjBuilder timeBuilder(128);

            result.append("host", prettyHostName() );
            result.append("version", mongodbVersionString);
            result.append("tokumxVersion", tokumxVersionString);
            result.append("process","mongod");
            result.append("pid", (int)getpid());
            result.append("uptime",(double) (time(0)-cmdLine.started));
            result.append("uptimeMillis", (long long)(curTimeMillis64()-_started));
            result.append("uptimeEstimate",(double) (start/1000));
            result.appendDate( "localTime" , jsTime() );

            reportLockStats(result);

            {
                BSONObjBuilder t;
                
                t.append( "totalTime" , (long long)(1000 * ( curTimeMillis64() - _started ) ) );
                t.append( "lockTime" , Lock::globalLockStat()->getTimeLocked( 'W' ) );

                {
                    BSONObjBuilder ttt( t.subobjStart( "currentQueue" ) );
                    int w=0, r=0;
                    Client::getReaderWriterClientCount( &r , &w );
                    ttt.append( "total" , w + r );
                    ttt.append( "readers" , r );
                    ttt.append( "writers" , w );
                    ttt.done();
                }

                {
                    BSONObjBuilder ttt( t.subobjStart( "activeClients" ) );
                    int w=0, r=0;
                    Client::getActiveClientCount( w , r );
                    ttt.append( "total" , w + r );
                    ttt.append( "readers" , r );
                    ttt.append( "writers" , w );
                    ttt.done();
                }



                result.append( "globalLock" , t.obj() );
            }
            timeBuilder.appendNumber( "after basic" , Listener::getElapsedTimeMillis() - start );

            {
                BSONObjBuilder bb( result.subobjStart( "connections" ) );
                bb.append( "current" , Listener::globalTicketHolder.used() );
                bb.append( "available" , Listener::globalTicketHolder.available() );
                bb.append( "totalCreated" , Listener::globalConnectionNumber.load() );
                bb.done();
            }
            timeBuilder.appendNumber( "after connections" , Listener::getElapsedTimeMillis() - start );

            {
                BSONObjBuilder bb( result.subobjStart( "extra_info" ) );
                bb.append("note", "fields vary by platform");
                ProcessInfo p;
                p.getExtraInfo(bb);
                bb.done();
                timeBuilder.appendNumber( "after extra info" , Listener::getElapsedTimeMillis() - start );

            }

            {
                BSONObjBuilder bb( result.subobjStart( "cursors" ) );
                ClientCursor::appendStats( bb );
                bb.done();
            }

            {
                BSONObjBuilder bb( result.subobjStart( "network" ) );
                networkCounter.append( bb );
                bb.done();
            }


            timeBuilder.appendNumber( "after counters" , Listener::getElapsedTimeMillis() - start );

            if ( anyReplEnabled() ) {
                BSONObjBuilder bb( result.subobjStart( "repl" ) );
                appendReplicationInfo(bb, cmdObj["repl"].numberInt());
                bb.done();

                if ( ! _isMaster() ) {
                    result.append( "opcountersRepl" , replOpCounters.getObj() );
                }

                if (theReplSet) {
                    result.append( "replNetworkQueue", BackgroundSync::get()->getCounters());
                }
            }

            timeBuilder.appendNumber( "after repl" , Listener::getElapsedTimeMillis() - start );

            result.append( "opcounters" , globalOpCounters.getObj() );

            {
                BSONObjBuilder asserts( result.subobjStart( "asserts" ) );
                asserts.append( "regular" , assertionCount.regular );
                asserts.append( "warning" , assertionCount.warning );
                asserts.append( "msg" , assertionCount.msg );
                asserts.append( "user" , assertionCount.user );
                asserts.append( "rollovers" , assertionCount.rollovers );
                asserts.done();
            }

            timeBuilder.appendNumber( "after asserts" , Listener::getElapsedTimeMillis() - start );

            result.append( "writeBacksQueued" , ! writeBackManager.queuesEmpty() );

            timeBuilder.appendNumber( "after dur" , Listener::getElapsedTimeMillis() - start );

            {
                RamLog* rl = RamLog::get( "warnings" );
                massert(15880, "no ram log for warnings?" , rl);

                if (rl->lastWrite() >= time(0)-(10*60)){ // only show warnings from last 10 minutes
                    vector<const char*> lines;
                    rl->get( lines );

                    BSONArrayBuilder arr( result.subarrayStart( "warnings" ) );
                    for ( unsigned i=std::max(0,(int)lines.size()-10); i<lines.size(); i++ )
                        arr.append( lines[i] );
                    arr.done();
                }
            }

            timeBuilder.appendNumber( "at end" , Listener::getElapsedTimeMillis() - start );
            if ( Listener::getElapsedTimeMillis() - start > 1000 ) {
                BSONObj t = timeBuilder.obj();
                log() << "serverStatus was very slow: " << t << endl;
                result.append( "timing" , t );
            }

            return true;
        }
    } cmdServerStatus;

    class CmdEngineStatus : public WebInformationCommand {
    public:
        CmdEngineStatus() : WebInformationCommand("engineStatus") {}

        virtual void help( stringstream& help ) const {
            help << "returns TokuMX engine statistics";
        }

        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {} // No auth required
        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            // Get engine status from TokuMX.
            // Status is system-wide, so we ignore the dbname and fromRepl bit.
            storage::get_status(result);
            return true;
        }
    } cmdEngineStatus;

    class CmdShowPendingLockRequests : public WebInformationCommand {
    public:
        CmdShowPendingLockRequests() : WebInformationCommand("showPendingLockRequests") {}

        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::showPendingLockRequests);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        virtual void help( stringstream& help ) const {
            help << "returns a list of pending, document-level level lock requests";
        }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            storage::get_pending_lock_request_status(result);
            return true;
        }
    } cmdShowPendingLockRequests;

    class CmdShowLiveTransactions : public WebInformationCommand {
    public:
        CmdShowLiveTransactions() : WebInformationCommand("showLiveTransactions") {}

        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::showLiveTransactions);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        virtual void help( stringstream& help ) const {
            help << "returns a list of live transactions";
        }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            storage::get_live_transaction_status(result);
            return true;
        }
    } cmdShowLiveTransactions;

    class CmdCheckpoint : public Command {
    public:
        CmdCheckpoint() : Command("checkpoint") {}
        virtual bool slaveOk() const { return true; }
        virtual LockType locktype() const { return NONE; }
        virtual bool requiresSync() const { return true; }
        virtual bool needsTxn() const { return false; }
        virtual int txnFlags() const { return noTxnFlags(); }
        virtual bool canRunInMultiStmtTxn() const { return false; }
        virtual OpSettings getOpSettings() const { return OpSettings(); }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::checkpoint);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        virtual void help( stringstream& help ) const {
            help << "performs a checkpoint of all TokuMX dictionaries." << endl;
        }
        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            storage::checkpoint();
            return true;
        }
    } cmdCheckpoint;

    class CmdDiagLogging : public InformationCommand {
    public:
        CmdDiagLogging() : InformationCommand("diagLogging") { }
        // Vanilla mongo had this, I don't know why.  Seems like if they need a write lock it should be global?
        virtual LockType locktype() const { return NONE; }
        bool adminOnly() const { return true; }
        void help(stringstream& h) const { h << "http://dochub.mongodb.org/core/monitoring#MonitoringandDiagnostics-DatabaseRecord%2FReplay%28diagLoggingcommand%29"; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::diagLogging);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& dbname , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            int was = _diaglog.setLevel( cmdObj.firstElement().numberInt() );
            _diaglog.flush();
            if ( !cmdLine.quiet )
                tlog() << "CMD: diagLogging set to " << _diaglog.getLevel() << " from: " << was << endl;
            result.append( "was" , was );
            return true;
        }
    } cmddiaglogging;


    /* drop collection */
    class CmdDrop : public FileopsCommand {
    public:
        CmdDrop() : FileopsCommand("drop") { }
        virtual bool logTheOp() { return true; }
        virtual bool slaveOk() const { return false; }
        virtual bool adminOnly() const { return false; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::dropCollection);
            out->push_back(Privilege(dbname, actions));
        }
        virtual void help( stringstream& help ) const { help << "drop a collection\n{drop : <collectionName>}"; }
        virtual bool run(const string& dbname , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            string nsToDrop = dbname + '.' + cmdObj.firstElement().valuestr();
            if ( !cmdLine.quiet )
                tlog() << "CMD: drop " << nsToDrop << endl;
            uassert( 10039 ,  "can't drop collection with reserved $ character in name", strchr(nsToDrop.c_str(), '$') == 0 );
            NamespaceDetails *d = nsdetails(nsToDrop);
            if ( d == 0 ) {
                errmsg = "ns not found";
                return false;
            }
            dropCollection( nsToDrop, errmsg, result );
            return true;
        }
    } cmdDrop;

    /* select count(*) */
    class CmdCount : public QueryCommand {
    public:
        CmdCount() : QueryCommand("count") { }
        virtual bool slaveOk() const { return false; }
        virtual bool slaveOverrideOk() const { return true; }
        virtual bool maintenanceOk() const { return false; }
        virtual bool adminOnly() const { return false; }
        virtual void help( stringstream& help ) const { help << "count objects in collection"; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseNs(dbname, cmdObj), actions));
        }
        virtual bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            string ns = parseNs(dbname, cmdObj);
            string err;
            int errCode;
            long long n = runCount(ns.c_str(), cmdObj, err, errCode);
            long long nn = n;
            bool ok = true;
            if ( n == -1 ) {
                nn = 0;
                result.appendBool( "missing" , true );
            }
            else if ( n < 0 ) {
                nn = 0;
                ok = false;
                if ( !err.empty() ) {
                    errmsg = err;
                    return false;
                }
            }
            result.append("n", (double) nn);
            return ok;
        }
    } cmdCount;

    /* create collection */
    class CmdCreate : public FileopsCommand {
    public:
        CmdCreate() : FileopsCommand("create") { }
        virtual bool logTheOp() { return false; }
        virtual bool slaveOk() const { return false; }
        virtual bool adminOnly() const { return false; }
        virtual void help( stringstream& help ) const {
            help << "create a collection explicitly\n"
                "{ create: <ns>[, capped: <bool>, size: <collSizeInBytes>, max: <nDocs>] }";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::createCollection);
            out->push_back(Privilege(dbname, actions));
        }
        virtual bool run(const string& dbname , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            uassert(15888, "must pass name of collection to create", cmdObj.firstElement().valuestrsafe()[0] != '\0');
            string ns = dbname + '.' + cmdObj.firstElement().valuestr();
            string err;
            if (cmdObj["capped"].trueValue()) {
                uassert(14832, "specify size:<n> when capped is true", cmdObj["size"].isNumber() || (cmdObj["size"].type() == String) || cmdObj.hasField("$nExtents"));
            }
            bool ok = userCreateNS(ns.c_str(), cmdObj, err, ! fromRepl );
            if ( !ok && !err.empty() )
                errmsg = err;
            return ok;
        }
    } cmdCreate;

    /* "dropIndexes" is now the preferred form - "deleteIndexes" deprecated */
    class CmdDropIndexes : public FileopsCommand {
    public:
        CmdDropIndexes() : FileopsCommand("dropIndexes", false, "deleteIndexes") { }
        virtual bool logTheOp() { return true; }
        // TODO: maybe slaveOk should be true?
        virtual bool slaveOk() const { return false; }
        virtual void help( stringstream& help ) const {
            help << "drop indexes for a collection";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::dropIndexes);
            out->push_back(Privilege(parseNs(dbname, cmdObj), actions));
        }
        bool run(const string& dbname, BSONObj& jsobj, int, string& errmsg, BSONObjBuilder& anObjBuilder, bool /*fromRepl*/) {
            BSONElement e = jsobj.firstElement();
            string toDeleteNs = dbname + '.' + e.valuestr();
            NamespaceDetails *d = nsdetails(toDeleteNs.c_str());
            if ( !cmdLine.quiet )
                tlog() << "CMD: dropIndexes " << toDeleteNs << endl;
            if ( d ) {
                BSONElement f = jsobj.getField("index");
                if ( f.type() == String ) {
                    return d->dropIndexes( toDeleteNs.c_str(), f.valuestr(), errmsg, anObjBuilder, false );
                }
                else if ( f.type() == Object ) {
                    int idxId = d->findIndexByKeyPattern( f.embeddedObject() );
                    if ( idxId < 0 ) {
                        errmsg = "can't find index with key:";
                        errmsg += f.embeddedObject().toString();
                        return false;
                    }
                    else {
                        IndexDetails& ii = d->idx( idxId );
                        string iName = ii.indexName();
                        return d->dropIndexes( toDeleteNs.c_str(), iName.c_str() , errmsg, anObjBuilder, false );
                    }
                }
                else {
                    errmsg = "invalid index name spec";
                    return false;
                }
            }
            else {
                errmsg = "ns not found";
                return false;
            }
        }
    } cmdDropIndexes;

    class CmdReIndex : public ModifyCommand {
    public:
        CmdReIndex() : ModifyCommand("reIndex") { }
        virtual bool logTheOp() { return false; } // only reindexes on the one node
        virtual bool slaveOk() const { return true; }    // can reindex on a secondary
        virtual bool requiresSync() const { return false; }
        virtual void help( stringstream& help ) const {
            help << "re-index a collection";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::reIndex);
            out->push_back(Privilege(parseNs(dbname, cmdObj), actions));
        }
        bool run(const string& dbname , BSONObj& jsobj, int, string& errmsg, BSONObjBuilder& result, bool /*fromRepl*/) {
            static DBDirectClient db;

            BSONElement e = jsobj.firstElement();
            string toDeleteNs = dbname + '.' + e.valuestr();
            NamespaceDetails *d = nsdetails(toDeleteNs.c_str());
            tlog() << "CMD: reIndex " << toDeleteNs << endl;

            if ( ! d ) {
                errmsg = "ns not found";
                return false;
            }

            list<BSONObj> all;
            auto_ptr<DBClientCursor> i = db.query( getSisterNS(dbname, "system.indexes") , BSON( "ns" << toDeleteNs ) , 0 , 0 , 0 , QueryOption_SlaveOk );
            BSONObjBuilder b;
            while ( i->more() ) {
                BSONObj o = i->next().getOwned();
                b.append( BSONObjBuilder::numStr( all.size() ) , o );
                all.push_back( o );
            }

            // run optimize
            d->optimizeAll();

            result.append( "nIndexes" , (int)all.size() );
            // Vanilla mongo does a drop followed by an index build, and the drop populates this field.
            // It doesn't make much sense for us, but we report it anyway because a python test expected it.
            result.append( "nIndexesWas" , (int)all.size() );
            result.appendArray( "indexes" , b.obj() );
            return true;
        }
    } cmdReIndex;

    class CmdRenameCollection : public FileopsCommand {
    public:
        CmdRenameCollection() : FileopsCommand( "renameCollection" ) {}
        virtual bool adminOnly() const { return true; }
        virtual bool requiresAuth() { return true; }
        virtual bool lockGlobally() const { return true; }
        virtual bool slaveOk() const { return false; }
        virtual bool logTheOp() {
            return true; // can't log steps when doing fast rename within a db, so always log the op rather than individual steps comprising it.
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            rename_collection::addPrivilegesRequiredForRenameCollection(cmdObj, out);
        }
        virtual void help( stringstream &help ) const {
            help << " example: { renameCollection: foo.a, to: bar.b }";
        }
        virtual bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            string source = cmdObj.getStringField( name.c_str() );
            string target = cmdObj.getStringField( "to" );
            uassert(15967,"invalid collection name: " + target, NamespaceString::validCollectionName(target));
            if ( source.empty() || target.empty() ) {
                errmsg = "invalid command syntax";
                return false;
            }

            bool capped = false;
            long long size = 0;
            {
                Client::Context ctx( source );
                NamespaceDetails *nsd = nsdetails( source );
                uassert( 10026 ,  "source namespace does not exist", nsd );
                capped = nsd->isCapped();
                // TODO: Get the capped size
            }

            Client::Context ctx( target );

            if ( nsdetails( target.c_str() ) ) {
                uassert( 10027 ,  "target namespace exists", cmdObj["dropTarget"].trueValue() );
                BSONObjBuilder bb( result.subobjStart( "dropTarget" ) );
                dropCollection( target , errmsg , bb );
                bb.done();
                if ( errmsg.size() > 0 )
                    return false;
            }


            // if we are renaming in the same database, just
            // rename the namespace and we're done.
            {
                StringData from = nsToDatabaseSubstring(source);
                StringData to = nsToDatabaseSubstring(target);
                if ( from == to ) {
                    renameNamespace( source, target );
                    // make sure we drop counters etc
                    Top::global.collectionDropped( source );
                    return true;
                }
            }

            // renaming across databases, so we must copy all
            // the data and then remove the source collection.
            BSONObjBuilder spec;
            if ( capped ) {
                spec.appendBool( "capped", true );
                spec.append( "size", double( size ) );
            }
            if ( !userCreateNS( target, spec.done(), errmsg , false) )
                return false;

            auto_ptr< DBClientCursor > c;
            DBDirectClient bridge;

            {
                c = bridge.query( source, BSONObj(), 0, 0, 0, fromRepl ? QueryOption_SlaveOk : 0 );
            }
            while( 1 ) {
                {
                    if ( !c->more() )
                        break;
                }
                BSONObj o = c->next();
                // logop set to false, because command is logged
                insertObject( target.c_str(), o, 0, false );
            }

            string sourceIndexes = getSisterNS(source, "system.indexes");
            string targetIndexes = getSisterNS(target, "system.indexes");
            {
                c = bridge.query( sourceIndexes, QUERY( "ns" << source ), 0, 0, 0, fromRepl ? QueryOption_SlaveOk : 0 );
            }
            while( 1 ) {
                {
                    if ( !c->more() )
                        break;
                }
                BSONObj o = c->next();
                BSONObjBuilder b;
                BSONObjIterator i( o );
                while( i.moreWithEOO() ) {
                    BSONElement e = i.next();
                    if ( e.eoo() )
                        break;
                    if ( strcmp( e.fieldName(), "ns" ) == 0 ) {
                        b.append( "ns", target );
                    }
                    else {
                        b.append( e );
                    }
                }
                BSONObj n = b.done();
                // logop set to false, because command is logged
                insertObject( targetIndexes.c_str(), n, 0, false );
            }

            {
                Client::Context ctx( source );
                dropCollection( source, errmsg, result );
            }
            return true;
        }
    } cmdrenamecollection;

    class CmdListDatabases : public Command {
    public:
        virtual bool slaveOk() const { return true; }
        virtual bool slaveOverrideOk() const { return true; }
        virtual bool adminOnly() const { return true; }
        virtual LockType locktype() const { return READ; }
        virtual bool lockGlobally() const { return true; }
        virtual bool requiresSync() const { return false; }
        virtual bool needsTxn() const { return true; }
        virtual int txnFlags() const { return DB_TXN_READ_ONLY | DB_TXN_SNAPSHOT; }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual OpSettings getOpSettings() const { return OpSettings(); }

        virtual void help( stringstream& help ) const { help << "list databases on this server"; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::listDatabases);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdListDatabases() : Command("listDatabases" , true ) {}
        bool run(const string& dbname , BSONObj& jsobj, int, string& errmsg, BSONObjBuilder& result, bool /*fromRepl*/) {
            vector< string > dbNames;
            getDatabaseNames( dbNames );
            vector< BSONObj > dbInfos;

            set<string> seen;
            boost::intmax_t totalSize = 0;
            for ( vector< string >::iterator i = dbNames.begin(); i != dbNames.end(); ++i ) {
                BSONObjBuilder b;
                b.append( "name", *i );

                DEV LOG(0) << "don't know how to calculate the sizeOnDisk of a database yet!" << endl;
                boost::intmax_t size = 0;
                b.append( "sizeOnDisk", (double) size );
                totalSize += size;
                
                if (1) {
                    Client::ReadContext rc( getSisterNS(*i, "system.namespaces") );
                    b.appendBool( "empty", rc.ctx().db()->isEmpty() );
                }
                
                dbInfos.push_back( b.obj() );

                seen.insert( i->c_str() );
            }

            // TODO: erh 1/1/2010 I think this is broken where path != dbpath ??
            set<string> allShortNames;
            if (!jsobj.hasElement( "onDiskOnly" )) {
                Lock::GlobalRead lk;
                dbHolder().getAllShortNames( allShortNames );
            }
            
            for ( set<string>::iterator i = allShortNames.begin(); i != allShortNames.end(); i++ ) {
                string name = *i;

                if ( seen.count( name ) )
                    continue;

                BSONObjBuilder b;
                b.append( "name" , name );
                b.append( "sizeOnDisk" , (double)1.0 );

                if (1) {
                    Client::ReadContext ctx( name );
                    b.appendBool( "empty", ctx.ctx().db()->isEmpty() );
                }

                dbInfos.push_back( b.obj() );
            }

            result.append( "databases", dbInfos );
            result.append( "totalSize", double( totalSize ) );
            return true;
        }
    } cmdListDatabases;

    class CmdFileMD5 : public QueryCommand {
    public:
        CmdFileMD5() : QueryCommand( "filemd5" ) {}
        virtual void help( stringstream& help ) const {
            help << " example: { filemd5 : ObjectId(aaaaaaa) , root : \"fs\" }";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseNs(dbname, cmdObj), actions));
        }
        bool run(const string& dbname, BSONObj& jsobj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            string ns = dbname;
            ns += ".";
            {
                string root = jsobj.getStringField( "root" );
                if ( root.size() == 0 )
                    root = "fs";
                ns += root;
            }
            ns += ".chunks"; // make this an option in jsobj

            // Check shard version at startup.
            // This will throw before we've done any work if shard version is outdated
            Client::Context ctx (ns);

            md5digest d;
            md5_state_t st;
            md5_init(&st);

            int n = 0;

            bool partialOk = jsobj["partialOk"].trueValue();
            if (partialOk) {
                // WARNING: This code depends on the binary layout of md5_state. It will not be
                // compatible with different md5 libraries or work correctly in an environment with
                // mongod's of different endians. It is ok for mongos to be a different endian since
                // it just passes the buffer through to another mongod.
                BSONElement stateElem = jsobj["md5state"];
                if (!stateElem.eoo()){
                    int len;
                    const char* data = stateElem.binDataClean(len);
                    massert(16247, "md5 state not correct size", len == sizeof(st));
                    memcpy(&st, data, sizeof(st));
                }
                n = jsobj["startAt"].numberInt();
            }


            BSONObj query = BSON( "files_id" << jsobj["filemd5"] << "n" << GTE << n );
            BSONObj sort = BSON( "files_id" << 1 << "n" << 1 );

            shared_ptr<Cursor> cursor = getBestGuessCursor(ns.c_str(), query, sort);
            if ( ! cursor ) {
                errmsg = "need an index on { files_id : 1 , n : 1 }";
                return false;
            }
            auto_ptr<ClientCursor> cc (new ClientCursor(QueryOption_NoCursorTimeout, cursor, ns.c_str()));

            for ( ; cursor->ok() ; cursor->advance() ) {
                if ( ! cursor->matcher()->matchesCurrent( cursor.get() ) ) {
                    log() << "**** NOT MATCHING ****" << endl;
                    PRINT(cursor->current());
                    continue;
                }

                BSONObj obj = cursor->current();
                BSONElement ne = obj["n"];
                verify(ne.isNumber());
                int myn = ne.numberInt();
                if ( n != myn ) {
                    if (partialOk) {
                        break; // skipped chunk is probably on another shard
                    }
                    log() << "should have chunk: " << n << " have:" << myn << endl;
                    dumpChunks( ns , query , sort );
                    uassert( 10040 ,  "chunks out of order" , n == myn );
                }

                int len;
                const char * data = obj["data"].binDataClean( len );

                md5_append( &st , (const md5_byte_t*)(data) , len );
                n++;
            }

            if (partialOk)
                result.appendBinData("md5state", sizeof(st), BinDataGeneral, &st);

            // This must be *after* the capture of md5state since it mutates st
            md5_finish(&st, d);

            result.append( "numChunks" , n );
            result.append( "md5" , digestToString( d ) );
            return true;
        }

        void dumpChunks( const string& ns , const BSONObj& query , const BSONObj& sort ) {
            DBDirectClient client;
            Query q(query);
            q.sort(sort);
            auto_ptr<DBClientCursor> c = client.query(ns, q);
            while(c->more())
                PRINT(c->nextSafe());
        }
    } cmdFileMD5;

    class CmdDatasize : public QueryCommand {
        virtual string parseNs(const string& dbname, const BSONObj& cmdObj) const { 
            return parseNsFullyQualified(dbname, cmdObj);
        }
    public:
        CmdDatasize() : QueryCommand( "dataSize", false, "datasize" ) {}
        virtual void help( stringstream &help ) const {
            help <<
                 "determine data size for a set of data in a certain range"
                 "\nexample: { dataSize:\"blog.posts\", keyPattern:{x:1}, min:{x:10}, max:{x:55} }"
                 "\nmin and max parameters are optional. They must either both be included or both omitted"
                 "\nkeyPattern is an optional parameter indicating an index pattern that would be useful"
                 "for iterating over the min/max bounds. If keyPattern is omitted, it is inferred from "
                 "the structure of min. "
                 "\nnote: This command may take a while to run";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseNs(dbname, cmdObj), actions));
        }
        bool run(const string& dbname, BSONObj& jsobj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            Timer timer;

            string ns = jsobj.firstElement().String();
            BSONObj min = jsobj.getObjectField( "min" );
            BSONObj max = jsobj.getObjectField( "max" );
            BSONObj keyPattern = jsobj.getObjectField( "keyPattern" );
            bool estimate = jsobj["estimate"].trueValue();

            Client::Context ctx( ns );
            NamespaceDetails *d = nsdetails(ns.c_str());

            if ( ! d /* || d->stats.nrecords == 0 */) {
                result.appendNumber( "size" , 0 );
                result.appendNumber( "numObjects" , 0 );
                result.append( "millis" , timer.millis() );
                return true;
            }

            result.appendBool( "estimate" , estimate );

            shared_ptr<Cursor> c;
            if ( min.isEmpty() && max.isEmpty() ) {
#if 0
                if ( estimate ) {
                    result.appendNumber( "size" , d->stats.datasize );
                    result.appendNumber( "numObjects" , d->stats.nrecords );
                    result.append( "millis" , timer.millis() );
                    return 1;
                }
#endif
                NamespaceDetails *d = nsdetails( ns.c_str() );
                c =  BasicCursor::make( d );
            }
            else if ( min.isEmpty() || max.isEmpty() ) {
                errmsg = "only one of min or max specified";
                return false;
            }
            else {

                if ( keyPattern.isEmpty() ){
                    Helpers::toKeyFormat( min , keyPattern );
                }

                const IndexDetails *idx = d->findIndexByPrefix( keyPattern ,
                                                                true );  /* require single key */
                if ( idx == NULL ) {
                    errmsg = "couldn't find valid index containing key pattern";
                    return false;
                }
                // If both min and max non-empty, append MinKey's to make them fit chosen index
                min = Helpers::modifiedRangeBound( min , idx->keyPattern() , -1 );
                max = Helpers::modifiedRangeBound( max , idx->keyPattern() , -1 );

                c = IndexCursor::make( d, *idx, min, max, false, 1 );
            }

            //long long avgObjSize = d->stats.datasize / d->stats.nrecords;

            long long maxSize = jsobj["maxSize"].numberLong();
            long long maxObjects = jsobj["maxObjects"].numberLong();

            long long size = 0;
            long long numObjects = 0;
            while( c->ok() ) {

                // TODO: If estimate, use avgObjSize
                size += c->current().objsize();

                numObjects++;

                if ( ( maxSize && size > maxSize ) ||
                        ( maxObjects && numObjects > maxObjects ) ) {
                    result.appendBool( "maxReached" , true );
                    break;
                }

                c->advance();
            }

            ostringstream os;
            os <<  "Finding size for ns: " << ns;
            if ( ! min.isEmpty() ) {
                os << " between " << min << " and " << max;
            }
            logIfSlow( timer , os.str() );

            result.appendNumber( "size", size );
            result.appendNumber( "numObjects" , numObjects );
            result.append( "millis" , timer.millis() );
            return true;
        }
    } cmdDatasize;

    class CollectionStats : public QueryCommand {
    public:
        CollectionStats() : QueryCommand( "collStats", false, "collstats" ) {}
        virtual void help( stringstream &help ) const {
            help << "{ collStats:\"blog.posts\" , scale : 1 } scale divides sizes e.g. for KB use 1024\n"
                    "    avgObjSize - in bytes";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::collStats);
            out->push_back(Privilege(parseNs(dbname, cmdObj), actions));
        }
        bool run(const string& dbname, BSONObj& jsobj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            string ns = dbname + "." + jsobj.firstElement().valuestr();
            Client::Context cx( ns );

            NamespaceDetails * nsd = nsdetails( ns.c_str() );
            if ( ! nsd ) {
                errmsg = "ns not found";
                return false;
            }

            result.append( "ns" , ns.c_str() );

            int scale = 1;
            if ( jsobj["scale"].isNumber() ) {
                scale = jsobj["scale"].numberInt();
                if ( scale <= 0 ) {
                    errmsg = "scale has to be >= 1";
                    return false;
                }
            }
            else if ( jsobj["scale"].trueValue() ) {
                errmsg = "scale has to be a number >= 1";
                return false;
            }

            struct NamespaceDetailsAccStats accStats;
            nsd->fillCollectionStats(&accStats, &result, scale);

            return true;
        }
    } cmdCollectionStats;

    class DBStats : public QueryCommand {
    public:
        DBStats() : QueryCommand( "dbStats", false, "dbstats" ) {}
        virtual void help( stringstream &help ) const {
            help << 
                "Get stats on a database. Not instantaneous. Slower for databases with large .ns files.\n" << 
                "Example: { dbStats:1, scale:1 }";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::dbStats);
            out->push_back(Privilege(dbname, actions));
        }
        bool run(const string& dbname, BSONObj& jsobj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            int scale = 1;
            if ( jsobj["scale"].isNumber() ) {
                scale = jsobj["scale"].numberInt();
                if ( scale <= 0 ) {
                    errmsg = "scale has to be > 0";
                    return false;
                }
            }
            else if ( jsobj["scale"].trueValue() ) {
                errmsg = "scale has to be a number > 0";
                return false;
            }

            list<string> collections;
            NamespaceIndex *ni = nsindex(dbname.c_str());
            if ( ni != NULL ) {
                ni->getNamespaces( collections );
            }

            uint64_t ncollections = 0;
            uint64_t objects = 0;
            uint64_t size = 0;
            uint64_t storageSize = 0;
            uint64_t indexes = 0;
            uint64_t indexSize = 0;
            uint64_t indexStorageSize = 0;

            for (list<string>::const_iterator it = collections.begin(); it != collections.end(); ++it) {
                const string ns = *it;

                NamespaceDetails * nsd = nsdetails( ns.c_str() );
                if ( ! nsd ) {
                    errmsg = "missing ns: ";
                    errmsg += ns;
                    return false;
                }

                ncollections += 1;
                indexes += nsd->nIndexes();
                BSONObjBuilder dummy;
                struct NamespaceDetailsAccStats accStats;
                nsd->fillCollectionStats(&accStats, &dummy, scale);
                objects += accStats.count;
                size += accStats.size;
                storageSize += accStats.storageSize;
                indexSize += accStats.indexSize;
                indexStorageSize += accStats.indexStorageSize;
            }
            
            result.append      ( "db" , dbname );
            result.appendNumber( "collections" , (long long) ncollections );
            result.appendNumber( "objects" , (long long) objects );
            result.append      ( "avgObjSize" , objects == 0 ? 0 : double(size) / double(objects) );
            result.appendNumber( "dataSize" , (long long) size / scale );
            result.appendNumber( "storageSize" , (long long) storageSize / scale);
            result.appendNumber( "indexes" , (long long) indexes );
            result.appendNumber( "indexSize" , (long long) indexSize / scale );
            result.appendNumber( "indexStorageSize" , (long long) indexStorageSize / scale );
            return true;
        }
    } cmdDBStats;

    /* Returns client's uri */
    class CmdWhatsMyUri : public InformationCommand {
    public:
        CmdWhatsMyUri() : InformationCommand("whatsmyuri") { }
        virtual void help( stringstream &help ) const {
            help << "{whatsmyuri:1}";
        }
        virtual bool requiresAuth() { return false; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {} // No auth required
        virtual bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            BSONObj info = cc().curop()->info();
            result << "you" << info[ "client" ];
            return true;
        }
    } cmdWhatsMyUri;

    class DBHashCmd : public QueryCommand {
    public:
        DBHashCmd() : QueryCommand( "dbHash", false, "dbhash" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::dbHash);
            out->push_back(Privilege(dbname, actions));
        }
        virtual bool run(const string& dbname , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            list<string> colls;
            NamespaceIndex *ni = nsindex(dbname.c_str());
            if ( ni != NULL )
                ni->getNamespaces( colls );
            colls.sort();

            result.appendNumber( "numCollections" , (long long)colls.size() );
            result.append( "host" , prettyHostName() );

            md5_state_t globalState;
            md5_init(&globalState);

            BSONObjBuilder bb( result.subobjStart( "collections" ) );
            for ( list<string>::iterator i=colls.begin(); i != colls.end(); i++ ) {
                string c = *i;
                if ( nsToCollectionSubstring(c) == "system.profile" ) {
                    continue;
                }

                NamespaceDetails * nsd = nsdetails( c.c_str() );

                // debug SERVER-761
                NamespaceDetails::IndexIterator ii = nsd->ii();
                while( ii.more() ) {
                    const IndexDetails &idx = ii.next();
                    if ( !idx.info().isValid() ) {
                        log() << "invalid index for ns: " << c << " " << idx.info();
                        log() << endl;
                    }
                }

                if ( NamespaceString::isSystem(c) ) {
                    continue;
                }

                md5_state_t st;
                md5_init(&st);

                for (shared_ptr<Cursor> cursor(BasicCursor::make(nsd)); cursor->ok(); cursor->advance()) {
                    BSONObj curObj = cursor->current();
                    md5_append( &st , (const md5_byte_t*)curObj.objdata() , curObj.objsize() );
                }
                md5digest d;
                md5_finish(&st, d);
                string hash = digestToString( d );

                bb.append( c.c_str() + ( dbname.size() + 1 ) , hash );

                md5_append( &globalState , (const md5_byte_t*)hash.c_str() , hash.size() );
            }
            bb.done();

            md5digest d;
            md5_finish(&globalState, d);
            string hash = digestToString( d );

            result.append( "md5" , hash );

            return 1;
        }

    } dbhashCmd;

    /* for diagnostic / testing purposes. Enabled via command line. */
    class CmdSleep : public InformationCommand {
    public:
        CmdSleep() : InformationCommand("sleep") { }
        virtual bool adminOnly() const { return true; }
        virtual void help( stringstream& help ) const {
            help << "internal testing command.  Makes db block (in a read lock) for 100 seconds\n";
            help << "w:true write lock. secs:<seconds>";
        }
        // No auth needed because it only works when enabled via command line.
        virtual bool requiresAuth() { return false; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {}
        bool run(const string& ns, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            log() << "test only command sleep invoked" << endl;
            int secs = 100;
            if ( cmdObj["secs"].isNumber() )
                secs = cmdObj["secs"].numberInt();
            if( cmdObj.getBoolField("w") ) {
                Lock::GlobalWrite lk;
                sleepsecs(secs);
            }
            else {
                Lock::GlobalRead lk;
                sleepsecs(secs);
            }
            return true;
        }
    };
    MONGO_INITIALIZER(RegisterSleepCmd)(InitializerContext* context) {
        if (Command::testCommandsEnabled) {
            // Leaked intentionally: a Command registers itself when constructed.
            new CmdSleep();
        }
        return Status::OK();
    }

    // Testing-only, enabled via command line.
    class EmptyCapped : public ModifyCommand {
    public:
        EmptyCapped() : ModifyCommand( "emptycapped" ) {}
        virtual bool logTheOp() { return true; }
        // No auth needed because it only works when enabled via command line.
        virtual bool requiresAuth() { return false; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {}
        virtual bool run(const string& dbname , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            string coll = cmdObj[ "emptycapped" ].valuestrsafe();
            uassert( 13428, "emptycapped must specify a collection", !coll.empty() );
            string ns = dbname + "." + coll;
            NamespaceDetails *nsd = nsdetails( ns.c_str() );
            massert( 13429, "emptycapped no such collection", nsd );
            massert( 13424, "collection must be capped", nsd->isCapped() );
            nsd->empty();
            return true;
        }
    };
    MONGO_INITIALIZER(RegisterEmptyCappedCmd)(InitializerContext* context) {
        if (Command::testCommandsEnabled) {
            // Leaked intentionally: a Command registers itself when constructed.
            new EmptyCapped();
        }
        return Status::OK();
    }

    bool _execCommand(Command *c,
                      const string& dbname,
                      BSONObj& cmdObj,
                      int queryOptions,
                      std::string& errmsg,
                      BSONObjBuilder& result,
                      bool fromRepl) {

        try {
            return c->run(dbname, cmdObj, queryOptions, errmsg, result, fromRepl);
        }
        catch ( SendStaleConfigException& e ){
            LOG(1) << "command failed because of stale config, can retry" << causedBy( e ) << endl;
            throw;
        }
        catch (RetryWithWriteLock &e) {
            uasserted(16796, str::stream() << 
                             "bug: Unhandled RetryWithWriteLock exception thrown during cmd: " << causedBy(e)
                             << ". Either a necessary collection was dropped manually, or you hit a bug. ");
        }
        catch ( DBException& e ) {

            // TODO: Rethrown errors have issues here, should divorce SendStaleConfigException from the DBException tree

            stringstream ss;
            ss << "exception: " << e.what();
            result.append( "errmsg" , ss.str() );
            result.append( "code" , e.getCode() );
            return false;
        }
    }

    static bool canRunCommand(
        Command * c,
        string& dbname,
        int queryOptions,
        bool fromRepl,
        std::string &errmsg,
        BSONObjBuilder& result
        )
    {
        bool canRunHere =
            isMaster( dbname.c_str() ) ||
            c->slaveOk() ||
            ( c->slaveOverrideOk() && ( queryOptions & QueryOption_SlaveOk ) ) ||
            fromRepl;

        if ( ! canRunHere ) {
            result.append( "note" , "from execCommand" );
            errmsg = "not master";
            return false;
        }

        if ( ! c->maintenanceOk() && theReplSet && ! isMaster( dbname.c_str() ) && ! theReplSet->isSecondary() ) {
            result.append( "note" , "from execCommand" );
            errmsg = "node is recovering";
            return false;
        }
        return true;
    }

    static bool runCommandWithNoDBLock(
        Command* c ,
        Client& client , int queryOptions ,
        BSONObj& cmdObj,
        std::string &errmsg,
        BSONObjBuilder& result,
        bool fromRepl,
        string dbname
        )
    {
        bool retval = false;
        // not sure of the semantics of running this without having a lock held
        if (!canRunCommand(c, dbname, queryOptions, fromRepl, errmsg, result)) {
            return false;
        }
        verify(!c->lockGlobally());

        // This assert means your command has LockType NONE but thinks it needs a transaction.
        // You shouldn't be making a transaction without a lock to protect metadata, so your
        // command is probably broken.
        dassert(!c->needsTxn());
        
        // we also trust that this won't crash
        retval = true;

        if (retval) {
            client.curop()->ensureStarted();
            retval = _execCommand(c, dbname, cmdObj, queryOptions, errmsg, result, fromRepl);
        }
        return retval;
    }

    /**
     * this handles
     - auth
     - maintenance mode
     - locking
     - context
     then calls run()
    */
    void Command::execCommand(Command * c ,
                              Client& client,
                              int queryOptions,
                              const char *cmdns,
                              BSONObj& cmdObj,
                              BSONObjBuilder& result,
                              bool fromRepl ) {

        std::string dbname = nsToDatabase( cmdns );

        if (c->adminOnly() && c->localHostOnlyIfNoAuth(cmdObj) && noauth &&
                !client.getIsLocalHostConnection()) {
            log() << "command denied: " << cmdObj.toString() << endl;
            appendCommandStatus(result,
                                false,
                                "unauthorized: this command must run from localhost when running "
                                "db without auth");
            return;
        }

        if ( c->adminOnly() && ! fromRepl && dbname != "admin" ) {
            log() << "command denied: " << cmdObj.toString() << endl;
            appendCommandStatus(result, false, "access denied; use admin db");
            return;
        }

        if (!noauth && c->requiresAuth()) {
            std::vector<Privilege> privileges;
            c->addRequiredPrivileges(dbname, cmdObj, &privileges);
            Status status = client.getAuthorizationManager()->checkAuthForPrivileges(privileges);
            if (!status.isOK()) {
                log() << "command denied: " << cmdObj.toString() << endl;
                appendCommandStatus(result, false, status.reason());
                return;
            }
        }

        if ( cmdObj["help"].trueValue() ) {
            client.curop()->ensureStarted();
            stringstream ss;
            ss << "help for: " << c->name << " ";
            c->help( ss );
            result.append( "help" , ss.str() );
            result.append( "lockType" , c->locktype() );
            result.appendBool("requiresSync", c->requiresSync());
            appendCommandStatus(result, true, "");
            return;
        }

        LOG(c->adminOnly() ? 2 : 3) << "command: " << cmdObj << endl;

        // before we start this command, check if we can run in a multi statement transaction
        // If we cannot and are in a multi statement transaction, 
        // then we must automatically commit the multi statement transaction
        // before proceeding
        if (!fromRepl && !c->canRunInMultiStmtTxn()) {
            uassert(16786, "cannot run command inside of multi statement transaction", !cc().hasTxn());
        }

        std::string errmsg;
        bool retval = false;
        OpSettings settings = c->getOpSettings();
        cc().setOpSettings(settings);
        if ( c->locktype() == Command::NONE ) {
            retval = runCommandWithNoDBLock(c, client, queryOptions, cmdObj, errmsg, result, fromRepl, dbname);
        }
        else if ( c->locktype() == Command::OPLOCK ) {
            RWLockRecursive::Shared lk(operationLock);
            retval = runCommandWithNoDBLock(c, client, queryOptions, cmdObj, errmsg, result, fromRepl, dbname);
        }
        else if( c->locktype() == Command::READ ) { 
            // read lock
            string ns = c->parseNs(dbname, cmdObj);

            // Establish a read context first, which will open the db if it is closed.
            //
            // If we end up needing a global read lock, then our ReadContext is not
            // sufficient. Upgrade to a global read lock. Note that the db may close
            // between lock acquisions, but that's okay - we'll uassert later in the
            // Client::Context constructor and the user must retry the command.
            scoped_ptr<Client::ReadContext> rctx(new Client::ReadContext(ns, dbpath));
            scoped_ptr<Lock::GlobalRead> lk;
            if (c->lockGlobally()) {
                rctx.reset();
                lk.reset(new Lock::GlobalRead());
            }
            Client::Context ctx(ns, dbpath);
            if (!canRunCommand(c, dbname, queryOptions, fromRepl, errmsg, result)) {
                appendCommandStatus(result, false, errmsg);
                return;
            }

            scoped_ptr<Client::Transaction> txn((!fromRepl && c->needsTxn())
                                                ? new Client::Transaction(c->txnFlags())
                                                : NULL);
            client.curop()->ensureStarted();
            retval = _execCommand(c, dbname, cmdObj, queryOptions, errmsg, result, fromRepl);

            if ( retval && c->logTheOp() && ! fromRepl ) {
                OpLogHelpers::logCommand(cmdns, cmdObj, &cc().txn());
            }

            if (retval && txn) {
                txn->commit();
            }
        }
        else {
            dassert( c->locktype() == Command::WRITE );
            bool global = c->lockGlobally();
            DEV {
                if( !global && Lock::isW() ) { 
                    log() << "\ndebug have W lock but w would suffice for command " << c->name << endl;
                }
                if( global && Lock::isLocked() == 'w' ) { 
                    // can't go w->W
                    log() << "need global W lock but already have w on command : " << cmdObj.toString() << endl;
                }
            }
            scoped_ptr<Lock::ScopedLock> lk(global
                                            ? static_cast<Lock::ScopedLock*>(new Lock::GlobalWrite())
                                            : static_cast<Lock::ScopedLock*>(new Lock::DBWrite(dbname)));
            if (!canRunCommand(c, dbname, queryOptions, fromRepl, errmsg, result)) {
                appendCommandStatus(result, false, errmsg);
                return;
            }

            Client::Context ctx(dbname, dbpath);
            scoped_ptr<Client::Transaction> transaction((!fromRepl && c->needsTxn())
                                                        ? new Client::Transaction(c->txnFlags())
                                                        : NULL);
            client.curop()->ensureStarted();
            retval = _execCommand(c, dbname, cmdObj, queryOptions, errmsg, result, fromRepl);
            if ( retval && c->logTheOp() && ! fromRepl ) {
                OpLogHelpers::logCommand(cmdns, cmdObj, &cc().txn());
            }

            if (retval && transaction) {
                transaction->commit();
            }
        }

        appendCommandStatus(result, retval, errmsg);
        return;
    }


    /* TODO make these all command objects -- legacy stuff here

       usage:
         abc.$cmd.findOne( { ismaster:1 } );

       returns true if ran a cmd
    */
    bool _runCommands(const char *ns, BSONObj& _cmdobj, BufBuilder &b, BSONObjBuilder& anObjBuilder, bool fromRepl, int queryOptions) {
        string dbname = nsToDatabase( ns );

        if( logLevel >= 1 )
            log() << "run command " << ns << ' ' << _cmdobj << endl;

        const char *p = strchr(ns, '.');
        if ( !p ) return false;
        if ( strcmp(p, ".$cmd") != 0 ) return false;

        BSONObj jsobj;
        {
            BSONElement e = _cmdobj.firstElement();
            if ( e.type() == Object && (e.fieldName()[0] == '$'
                                         ? str::equals("query", e.fieldName()+1)
                                         : str::equals("query", e.fieldName())))
            {
                jsobj = e.embeddedObject();
                if (_cmdobj.hasField("$readPreference")) {
                    queryOptions |= QueryOption_SlaveOk;
                }
            }
            else {
                jsobj = _cmdobj;
            }
        }

        // Treat the command the same as if it has slaveOk bit on if it has a read
        // preference setting. This is to allow these commands to run on a secondary.
        if (hasReadPreference(_cmdobj)) {
            queryOptions |= QueryOption_SlaveOk;
        }

        Client& client = cc();

        BSONElement e = jsobj.firstElement();

        Command * c = e.type() ? Command::findCommand( e.fieldName() ) : 0;

        if ( c ) {
            Command::execCommand(c, client, queryOptions, ns, jsobj, anObjBuilder, fromRepl);
        }
        else {
            Command::appendCommandStatus(anObjBuilder,
                                         false,
                                         str::stream() << "no such cmd: " << e.fieldName());
            anObjBuilder.append("bad cmd" , _cmdobj );
        }

        BSONObj x = anObjBuilder.done();
        b.appendBuf((void*) x.objdata(), x.objsize());

        return true;
    }

} // namespace mongo
