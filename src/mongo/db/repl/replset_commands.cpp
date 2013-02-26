/**
*    Copyright (C) 2008 10gen Inc.
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

#include "mongo/pch.h"

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "../cmdline.h"
#include "../commands.h"
#include "../repl.h"
#include "health.h"
#include "rs.h"
#include "rs_config.h"
#include "../dbwebserver.h"
#include "../../util/mongoutils/html.h"
#include "../repl_block.h"

using namespace bson;

namespace mongo {

    void checkMembersUpForConfigChange(const ReplSetConfig& cfg, BSONObjBuilder& result, bool initial);

    /* commands in other files:
         replSetHeartbeat - health.cpp
         replSetInitiate  - rs_mod.cpp
    */

    bool replSetBlind = false;
    unsigned replSetForceInitialSyncFailure = 0;

    // Testing only, enabled via command-line.
    class CmdReplSetTest : public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "Just for regression tests.\n";
        }
        // No auth needed because it only works when enabled via command line.
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {}
        CmdReplSetTest() : ReplSetCommand("replSetTest") { }
        virtual bool needsTxn() const { return false; }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            log() << "replSet replSetTest command received: " << cmdObj.toString() << rsLog;

            if( cmdObj.hasElement("forceInitialSyncFailure") ) {
                replSetForceInitialSyncFailure = (unsigned) cmdObj["forceInitialSyncFailure"].Number();
                return true;
            }

            if( !check(errmsg, result) )
                return false;

            if( cmdObj.hasElement("blind") ) {
                replSetBlind = cmdObj.getBoolField("blind");
                return true;
            }

            if (cmdObj.hasElement("sethbmsg")) {
                sethbmsg(cmdObj["sethbmsg"].String());
                return true;
            }

            return false;
        }
    };
    MONGO_INITIALIZER(RegisterReplSetTestCmd)(InitializerContext* context) {
        if (Command::testCommandsEnabled) {
            // Leaked intentionally: a Command registers itself when constructed.
            new CmdReplSetTest();
        }
        return Status::OK();
    }

    /** get rollback id.  used to check if a rollback happened during some interval of time.
        as consumed, the rollback id is not in any particular order, it simply changes on each rollback.
        @see incRBID()
    */
    class CmdReplSetGetRBID : public ReplSetCommand {
    public:
        /* todo: ideally this should only change on rollbacks NOT on mongod restarts also. fix... */
        int rbid;
        virtual void help( stringstream &help ) const {
            help << "internal";
        }
        CmdReplSetGetRBID() : ReplSetCommand("replSetGetRBID") {
            // this is ok but micros or combo with some rand() and/or 64 bits might be better --
            // imagine a restart and a clock correction simultaneously (very unlikely but possible...)
            rbid = (int) curTimeMillis64();
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetGetRBID);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( !check(errmsg, result) )
                return false;
            result.append("rbid",rbid);
            return true;
        }
    } cmdReplSetRBID;

    /** we increment the rollback id on every rollback event. */
    void incRBID() {
        cmdReplSetRBID.rbid++;
    }

    /** helper to get rollback id from another server. */
    int getRBID(DBClientConnection *c) {
        bo info;
        c->simpleCommand("admin", &info, "replSetGetRBID");
        return info["rbid"].numberInt();
    }

    class CmdReplSetGetStatus : public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "Report status of a replica set from the POV of this server\n";
            help << "{ replSetGetStatus : 1 }";
            help << "\nhttp://dochub.mongodb.org/core/replicasetcommands";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetGetStatus);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetGetStatus() : ReplSetCommand("replSetGetStatus", true) { }
        virtual bool needsTxn() const { return false; }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if ( cmdObj["forShell"].trueValue() )
                lastError.disableForCommand();

            if( !check(errmsg, result) )
                return false;
            theReplSet->summarizeStatus(result);
            return true;
        }
    } cmdReplSetGetStatus;

    class CmdReplSetReconfig : public ReplSetCommand {
        RWLock mutex; /* we don't need rw but we wanted try capability. :-( */
    public:
        virtual void help( stringstream &help ) const {
            help << "Adjust configuration of a replica set\n";
            help << "{ replSetReconfig : config_object }";
            help << "\nhttp://dochub.mongodb.org/core/replicasetcommands";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetReconfig);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetReconfig() : ReplSetCommand("replSetReconfig"), mutex("rsreconfig") { }
        virtual bool needsTxn() const { return false; }
        virtual bool canRunInMultiStmtTxn() const { return false; }
        virtual bool run(const string& a, BSONObj& b, int e, string& errmsg, BSONObjBuilder& c, bool d) {
            try {
                rwlock_try_write lk(mutex);
                return _run(a,b,e,errmsg,c,d);
            }
            catch(rwlock_try_write::exception&) { }
            errmsg = "a replSetReconfig is already in progress";
            return false;
        }
    private:
        bool _run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( cmdObj["replSetReconfig"].type() != Object ) {
                errmsg = "no configuration specified";
                return false;
            }

            // We might want to add the protocol version of theReplSet->config() if it exists,
            // instead of just blindly adding our compiled-in CURRENT_PROTOCOL_VERSION.  But for
            // TokuMX 1.0 it doesn't matter.
            BSONObj configObj = ReplSetConfig::addProtocolVersionIfMissing(cmdObj["replSetReconfig"].Obj());

            bool force = cmdObj.hasField("force") && cmdObj["force"].trueValue();
            if( force && !theReplSet ) {
                replSettings.reconfig = configObj.getOwned();
                result.append("msg", "will try this config momentarily, try running rs.conf() again in a few seconds");
                return true;
            }

            if ( !check(errmsg, result) ) {
                return false;
            }

            if( !force && !theReplSet->box.getState().primary() ) {
                errmsg = "replSetReconfig command must be sent to the current replica set primary.";
                return false;
            }

            {
                // just make sure we can get a write lock before doing anything else.  we'll reacquire one
                // later.  of course it could be stuck then, but this check lowers the risk if weird things
                // are up - we probably don't want a change to apply 30 minutes after the initial attempt.
                time_t t = time(0);
                LOCK_REASON(lockReason, "repl: testing write lock time");
                Lock::GlobalWrite lk(lockReason);
                if( time(0)-t > 20 ) {
                    errmsg = "took a long time to get write lock, so not initiating.  Initiate when server less busy?";
                    return false;
                }
            }

            try {
                scoped_ptr<ReplSetConfig> newConfig
                        (ReplSetConfig::make(configObj, force));

                log() << "replSet replSetReconfig config object parses ok, " <<
                        newConfig->members.size() << " members specified" << rsLog;

                if( !ReplSetConfig::legalChange(theReplSet->getConfig(), *newConfig, errmsg) ) {
                    return false;
                }

                checkMembersUpForConfigChange(*newConfig, result, false);

                log() << "replSet replSetReconfig [2]" << rsLog;

                theReplSet->haveNewConfig(*newConfig, true);
                ReplSet::startupStatusMsg.set("replSetReconfig'd");
            }
            catch( DBException& e ) {
                log() << "replSet replSetReconfig exception: " << e.what() << rsLog;
                throw;
            }
            catch( string& se ) {
                log() << "replSet reconfig exception: " << se << rsLog;
                errmsg = se;
                return false;
            }

            resetSlaveCache();
            return true;
        }
    } cmdReplSetReconfig;

    class CmdReplSetFreeze : public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "{ replSetFreeze : <seconds> }";
            help << "'freeze' state of member to the extent we can do that.  What this really means is that\n";
            help << "this node will not attempt to become primary until the time period specified expires.\n";
            help << "You can call again with {replSetFreeze:0} to unfreeze sooner.\n";
            help << "A process restart unfreezes the member also.\n";
            help << "\nhttp://dochub.mongodb.org/core/replicasetcommands";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetFreeze);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetFreeze() : ReplSetCommand("replSetFreeze") { }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( !check(errmsg, result) )
                return false;
            int secs = (int) cmdObj.firstElement().numberInt();
            if( theReplSet->freeze(secs) ) {
                if( secs == 0 )
                    result.append("info","unfreezing");
            }
            if( secs == 1 )
                result.append("warning", "you really want to freeze for only 1 second?");
            return true;
        }
    } cmdReplSetFreeze;

    class CmdReplSetStepDown: public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "{ replSetStepDown : <seconds> }\n";
            help << "Step down as primary.  Will not try to reelect self for the specified time period (1 minute if no numeric secs value specified).\n";
            help << "(If another member with same priority takes over in the meantime, it will stay primary.)\n";
            help << "http://dochub.mongodb.org/core/replicasetcommands";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetStepDown);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetStepDown() : ReplSetCommand("replSetStepDown") { }
        virtual bool canRunInMultiStmtTxn() const { return false; }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( !check(errmsg, result) )
                return false;
            if( !theReplSet->box.getState().primary() ) {
                errmsg = "not primary so can't step down";
                return false;
            }

            bool force = cmdObj.hasField("force") && cmdObj["force"].trueValue();

            // only step down if there is another node synced to within 10
            // seconds of this node
            if (!force) {
                uint64_t lastOp = theReplSet->gtidManager->getCurrTimestamp()/1000;
                uint64_t closest = theReplSet->lastOtherOpTime()/1000;
                GTID lastGTID = theReplSet->gtidManager->getLiveState();
                GTID otherLastGTID = theReplSet->lastOtherGTID();

                uint64_t diff = (lastOp > closest) ? lastOp - closest : 0;
                result.appendNumber("closest", closest);
                result.appendNumber("difference", diff);

                if (GTID::cmp(lastGTID, otherLastGTID) < 0) {
                    // not our problem, but we'll wait until thing settle down
                    errmsg = "someone is ahead of the primary?";
                    return false;
                }

                if (diff > 10) {
                    errmsg = "no secondaries within 10 seconds of my optime";
                    return false;
                }
            }

            int secs = (int) cmdObj.firstElement().numberInt();
            if( secs == 0 )
                secs = 60;
            return theReplSet->stepDown(secs);
        }
    } cmdReplSetStepDown;

    class CmdReplSetMaintenance: public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "{ replSetMaintenance : bool }\n";
            help << "Enable or disable maintenance mode.";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetMaintenance);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetMaintenance() : ReplSetCommand("replSetMaintenance") { }
        virtual bool needsTxn() const { return false; }
        virtual bool canRunInMultiStmtTxn() const { return false; }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( !check(errmsg, result) )
                return false;

            if (!theReplSet->setMaintenanceMode(cmdObj["replSetMaintenance"].trueValue(), errmsg)) {
                return false;
            }

            return true;
        }
    } cmdReplSetMaintenance;

    class CmdReplSetSyncFrom: public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "{ replSetSyncFrom : \"host:port\" }\n";
            help << "Change who this member is syncing from.";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetSyncFrom);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetSyncFrom() : ReplSetCommand("replSetSyncFrom") { }
        virtual bool run(const string&, 
                         BSONObj& cmdObj, 
                         int, 
                         string& errmsg, 
                         BSONObjBuilder& result, 
                         bool fromRepl) {
            if (!check(errmsg, result)) {
                return false;
            }
            string newTarget = cmdObj["replSetSyncFrom"].valuestrsafe();
            result.append("syncFromRequested", newTarget);
            return theReplSet->forceSyncFrom(newTarget, errmsg, result);
        }
    } cmdReplSetSyncFrom;

    using namespace bson;
    using namespace mongoutils::html;
    extern void fillRsLog(stringstream&);

    class ReplSetHandler : public DbWebHandler {
    public:
        ReplSetHandler() : DbWebHandler( "_replSet" , 1 , true ) {}

        virtual bool handles( const string& url ) const {
            return startsWith( url , "/_replSet" );
        }

        virtual void handle( const char *rq, const std::string& url, BSONObj params,
                             string& responseMsg, int& responseCode,
                             vector<string>& headers,  const SockAddr &from ) {

            if( url == "/_replSetOplog" ) {
                responseMsg = _replSetOplog(params);
            }
            else
                responseMsg = _replSet();
            responseCode = 200;
        }

        string _replSetOplog(bo parms) {
            int _id = (int) str::toUnsigned( parms["_id"].String() );

            stringstream s;
            string t = "Replication oplog";
            s << start(t);
            s << p(t);

            if( theReplSet == 0 ) {
                if( cmdLine._replSet.empty() )
                    s << p("Not using --replSet");
                else  {
                    s << p("Still starting up, or else set is not yet " + a("http://dochub.mongodb.org/core/replicasetconfiguration#ReplicaSetConfiguration-InitialSetup", "", "initiated")
                           + ".<br>" + ReplSet::startupStatusMsg.get());
                }
            }
            else {
                try {
                    theReplSet->getOplogDiagsAsHtml(_id, s);
                }
                catch(std::exception& e) {
                    s << "error querying oplog: " << e.what() << '\n';
                }
            }

            s << _end();
            return s.str();
        }

        /* /_replSet show replica set status in html format */
        string _replSet() {
            stringstream s;
            s << start("Replica Set Status " + prettyHostName());
            s << p( a("/", "back", "Home") + " | " +
                    a("/local/system.replset/?html=1", "", "View Replset Config") + " | " +
                    a("/replSetGetStatus?text=1", "", "replSetGetStatus") + " | " +
                    a("http://dochub.mongodb.org/core/replicasets", "", "Docs")
                  );

            if( theReplSet == 0 ) {
                if( cmdLine._replSet.empty() )
                    s << p("Not using --replSet");
                else  {
                    s << p("Still starting up, or else set is not yet " + a("http://dochub.mongodb.org/core/replicasetconfiguration#ReplicaSetConfiguration-InitialSetup", "", "initiated")
                           + ".<br>" + ReplSet::startupStatusMsg.get());
                }
            }
            else {
                try {
                    theReplSet->summarizeAsHtml(s);
                }
                catch(...) { s << "error summarizing replset status\n"; }
            }
            s << p("Recent replset log activity:");
            fillRsLog(s);
            s << _end();
            return s.str();
        }



    } replSetHandler;

    class CmdReplSetExpireOplog : public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "set expireOplogDays and expireOplogHours\n";
            help << "{ replSetExpireOplog : 1, expireOplogDays:new_val, expireOplogHours:new_val }";
        }

        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetExpireOplog);
            out->push_back(Privilege(AuthorizationManager::CLUSTER_RESOURCE_NAME, actions));
        }

        CmdReplSetExpireOplog() : ReplSetCommand("replSetExpireOplog") { }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( cmdObj.hasElement("expireOplogDays") || cmdObj.hasElement("expireOplogHours") ) {
                uint32_t expireOplogDays = cmdLine.expireOplogDays;
                uint32_t expireOplogHours = cmdLine.expireOplogHours;
                if (cmdObj.hasElement("expireOplogHours")) {
                    BSONElement e = cmdObj["expireOplogHours"];
                    if (!e.isNumber()) {
                        errmsg = "bad expireOplogHours";
                        return false;
                    }
                    expireOplogHours = e.numberLong();
                }
                if (cmdObj.hasElement("expireOplogDays")) {
                    BSONElement e = cmdObj["expireOplogDays"];
                    if (!e.isNumber()) {
                        errmsg = "bad expireOplogDays";
                        return false;
                    }
                    expireOplogDays = e.numberLong();
                }
                theReplSet->changeExpireOplog(expireOplogDays, expireOplogHours);
            }
            return true;
        }
    } cmdReplSetExpireOplog;

    class CmdReplGetExpireOplog : public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "retrieve settings for expire oplog.\n";
            help << "{ replGetExpireOplog : 1 }";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replGetExpireOplog);
            out->push_back(Privilege(AuthorizationManager::CLUSTER_RESOURCE_NAME, actions));
        }

        CmdReplGetExpireOplog() : ReplSetCommand("replGetExpireOplog") { }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            // these reads can be racy. It does not matter
            result.append("expireOplogDays", cmdLine.expireOplogDays);
            result.append("expireOplogHours", cmdLine.expireOplogHours);
            return true;
        }
    } cmdReplGetExpireOplog;

    class CmdReplUndoOplogEntry : public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "internal\n";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replUndoOplogEntry);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }

        CmdReplUndoOplogEntry() : ReplSetCommand("replUndoOplogEntry") { }

        // This command is not meant to be run in a concurrent manner. Assumes user is running this in
        // a controlled setting.
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if (!cmdObj.hasElement("GTID")) {
                errmsg = "missing GTID";
                return false;
            }
            // extract GTID that was passed in
            GTID gtid = getGTIDFromBSON("GTID", cmdObj);
            BSONObjBuilder q;
            addGTIDToBSON("_id", gtid, q);
            BSONObj oplogEntry;
            bool foundLocally = false;
            // now let's find the oplog entry
            {
                LOCK_REASON(lockReason, "repl: looking for oplog entry to undo");
                Client::ReadContext ctx(rsoplog, lockReason);
                Client::Transaction transaction(DB_TXN_READ_ONLY | DB_READ_UNCOMMITTED);
                foundLocally = Collection::findOne(rsoplog, q.done(), oplogEntry);
                transaction.commit();
            }
            if (!foundLocally) {
                errmsg = "GTID not found in oplog";
                return false;
            }
            try {
                bool purgeEntry = true;
                if (cmdObj.hasElement("keepEntry")) {
                    purgeEntry = false;
                }
                rollbackTransactionFromOplog(oplogEntry, purgeEntry);
            }
            catch (std::exception& e2) {
                log() << "Caught std::exception during replUndoOplogEntry" << e2.what() << endl;
                errmsg = "Caught exception, check logs";
                return false;
            }
            return true;
        }
    } cmdReplUndoOplogEntry;

    class CmdLogReplInfo : public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "internal\n";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::logReplInfo);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }

        CmdLogReplInfo() : ReplSetCommand("logReplInfo") { }

        // This command is not meant to be run in a concurrent manner. Assumes user is running this in
        // a controlled setting.
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if (!cmdObj.hasElement("minLiveGTID") || !cmdObj.hasElement("minUnappliedGTID")) {
                errmsg = "missing either minLiveGTID or minUnappliedGTID";
                return false;
            }            
            if( replSet ) {
                errmsg = "This should only run without replset running";
                return false;
            }
            // extract GTID that was passed in
            GTID minLiveGTID = getGTIDFromBSON("minLiveGTID", cmdObj);
            GTID minUnappliedGTID = getGTIDFromBSON("minUnappliedGTID", cmdObj);
            if (GTID::cmp(minUnappliedGTID, minLiveGTID) > 0) {
                errmsg = "minUnappliedGTID cannot be greater than minLiveGTID";
                return false;
            }

            LOCK_REASON(lockReason, "repl: logging info");
            Lock::DBRead lk("local", lockReason);
            Client::Transaction transaction(DB_SERIALIZABLE);
            logToReplInfo(minLiveGTID, minUnappliedGTID);
            transaction.commit();

            return true;
        }
    } cmdLogReplInfo;

    class CmdReplAddPartition : public ReplSetCommand {
    public:
        virtual bool canRunInMultiStmtTxn() const { return false; }
        virtual void help( stringstream &help ) const {
            help << "add a partition to the oplog and oplog.refs collections\n";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replAddPartition);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
    
        CmdReplAddPartition() : ReplSetCommand("replAddPartition") { }
    
        // This command is not meant to be run in a concurrent manner. Assumes user is running this in
        // a controlled setting.
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            addOplogPartitions();
            return true;
        }
    } cmdReplAddPartition;

    class CmdReplTrimOplog: public ReplSetCommand {
    public:
        virtual bool canRunInMultiStmtTxn() const { return false; }
        virtual void help( stringstream &help ) const {
            // TODO: add more here
            help << "trim oplog and oplog.refs collections\n" <<
                "Either pass {ts : Date} or {GTID : gtid}";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replTrimOplog);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }

        CmdReplTrimOplog() : ReplSetCommand("replTrimOplog") { }

        // This command is not meant to be run in a concurrent manner. Assumes user is running this in
        // a controlled setting.
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            BSONElement tse = cmdObj["ts"];
            BSONElement gtide = cmdObj["gtid"];
            if (tse.ok() && gtide.ok()) {
                errmsg = "Can supply either gtid or ts, but not both";
                return false;
            }
            if (!tse.ok() && !gtide.ok()) {
                errmsg = "Must supply either ts or gtid as parameter for trimming";
                return false;
            }
            if (tse.ok()) {
                if (tse.type() != mongo::Date) {
                    errmsg = "Must supply a date for the ts field";
                    return false;
                }
                trimOplogWithTS(tse._numberLong());
            }
            else if (gtide.ok()) {
                // do some sanity checks
                if (!isValidGTID(gtide)) {
                    errmsg = "gtid is not valid and cannot be parsed";
                    return false;
                }
                GTID gtid = getGTIDFromBSON("gtid",cmdObj);
                trimOplogwithGTID(gtid);
            }
            return true;
        }
    } cmdTrimOplog;
}
