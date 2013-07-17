// commands.h

/*    Copyright 2009 10gen Inc.
 *    Copyright (C) 2013 Tokutek Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#pragma once

#include <db.h>

#include <vector>

#include "mongo/base/status.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/client_basic.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/opsettings.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    class BSONObj;
    class BSONObjBuilder;
    class Client;
    class Timer;

namespace mutablebson {
    class Document;
}  // namespace mutablebson

    /** mongodb "commands" (sent via db.$cmd.findOne(...))
        subclass to make a command.  define a singleton object for it.
        */
    class Command {
    protected:
        string parseNsFullyQualified(const string& dbname, const BSONObj& cmdObj) const;
    public:
        // only makes sense for commands where 1st parm is the collection.
        virtual string parseNs(const string& dbname, const BSONObj& cmdObj) const;

        // warning: isAuthorized uses the lockType() return values, and values are being passed 
        // around as ints so be careful as it isn't really typesafe and will need cleanup later
        //
        // the above comment makes adding OPLOCK dangerous. Looking at the code,
        // only isAuthorizedForLock in mongo/db/security.h and Top::_record in
        // db/stats/top.cpp use locktypes as ints, and for both cases, having OPLOCK
        // be -2 (as opposed to 2) works, as they both care whether the lock is WRITE
        // or not
        enum LockType { OPLOCK = -2, READ = -1 , NONE = 0 , WRITE = 1 };

        const string name;

        /* run the given command
           implement this...

           fromRepl - command is being invoked as part of replication syncing.  In this situation you
                      normally do not want to log the command to the local oplog.

           return value is true if succeeded.  if false, set errmsg text.
        */
        virtual bool run(const string& db, BSONObj& cmdObj, int options, string& errmsg, BSONObjBuilder& result, bool fromRepl = false ) = 0;

        /*
           note: logTheOp() MUST be false if READ
           if NONE, can't use Client::Context setup
                    use with caution
         */
        virtual LockType locktype() const = 0;

        /** whether the command should be run in a ShardedOperationScope

            usually true, except if the operation knows otherwise, like for setShardVersion (which
            will take the exclusive lock later) */
        virtual bool requiresShardedOperationScope() const { return true; }

        /** if true, this command must go to all members in a SyncClusterConnection.
            We add this because we can now have modifications without a locktype of WRITE, which is what we used to use to determine this for SyncClusterConnection.
         */
        virtual bool requiresSync() const = 0;

        /** if true, lock globally instead of just the one database. by default only the one 
            database will be locked. 
        */
        virtual bool lockGlobally() const { return false; }

        /** @return true iff this command wants a transaction */
        virtual bool needsTxn() const = 0;

        /** @return true iff this command can run in a multi statement transaction */
        virtual bool canRunInMultiStmtTxn() const = 0;

        virtual OpSettings getOpSettings() const {
            OpSettings settings;
            settings.setQueryCursorMode(DEFAULT_LOCK_CURSOR);
            return settings;
        }

        /** @return what transaction flags to use */
        virtual int txnFlags() const = 0;

        /* Return true if only the admin ns has privileges to run this command. */
        virtual bool adminOnly() const {
            return false;
        }

        void htmlHelp(stringstream&) const;

        /* Like adminOnly, but even stricter: we must either be authenticated for admin db,
           or, if running without auth, on the local interface.  Used for things which 
           are so major that remote invocation may not make sense (e.g., shutdownServer).

           When localHostOnlyIfNoAuth() is true, adminOnly() must also be true.
        */
        virtual bool localHostOnlyIfNoAuth(const BSONObj& cmdObj) { return false; }

        /* Return true if slaves are allowed to execute the command
           (the command directly from a client -- if fromRepl, always allowed).
        */
        virtual bool slaveOk() const = 0;

        /* Return true if the client force a command to be run on a slave by
           turning on the 'slaveOk' option in the command query.
        */
        virtual bool slaveOverrideOk() const {
            return false;
        }

        /* Override and return true to if true,log the operation (logOp()) to the replication log.
           (not done if fromRepl of course)

           Note if run() returns false, we do NOT log.
        */
        virtual bool logTheOp() { return false; }

        virtual void help( stringstream& help ) const;

        /**
         * Checks if the given client is authorized to run this command on database "dbname"
         * with the invocation described by "cmdObj".
         */
        virtual Status checkAuthForCommand(ClientBasic* client,
                                           const std::string& dbname,
                                           const BSONObj& cmdObj);

        /**
         * Redacts "cmdObj" in-place to a form suitable for writing to logs.
         *
         * The default implementation does nothing.
         */
        virtual void redactForLogging(mutablebson::Document* cmdObj);

        /* Return true if command should be permitted when a replica set secondary is in "recovering"
           (unreadable) state.
         */
        virtual bool maintenanceOk() const { return true; /* assumed true prior to commit */ }

        /** @param webUI expose the command in the web ui as localhost:28017/<name>
            @param oldName an optional old, deprecated name for the command
        */
        Command(const char *_name, bool webUI = false, const char *oldName = 0);

        virtual ~Command() {}

    protected:

        /**
         * Appends to "*out" the privileges required to run this command on database "dbname" with
         * the invocation described by "cmdObj".  New commands shouldn't implement this, they should
         * implement checkAuthForCommand instead.
         */
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            // The default implementation of addRequiredPrivileges should never be hit.
            fassertFailed(16940);
        }

        BSONObj getQuery( const BSONObj& cmdObj ) {
            if ( cmdObj["query"].type() == Object )
                return cmdObj["query"].embeddedObject();
            if ( cmdObj["q"].type() == Object )
                return cmdObj["q"].embeddedObject();
            return BSONObj();
        }

        static void logIfSlow( const Timer& cmdTimer,  const string& msg);

        static map<string,Command*> * _commands;
        static map<string,Command*> * _commandsByBestName;
        static map<string,Command*> * _webCommands;

        int noTxnFlags() const {
            msgasserted(16782, mongoutils::str::stream() << "can't call txnFlags with locktype NONE on command " << name);
            return 0;
        }

    public:
        static const map<string,Command*>* commandsByBestName() { return _commandsByBestName; }
        static const map<string,Command*>* webCommands() { return _webCommands; }
        /** @return if command was found */
        static void runAgainstRegistered(const char *ns,
                                         BSONObj& jsobj,
                                         BSONObjBuilder& anObjBuilder,
                                         int queryOptions = 0);
        static LockType locktype( const string& name );
        static Command * findCommand( const string& name );
        // For mongod and webserver.
        static void execCommand(Command* c,
                                Client& client,
                                int queryOptions,
                                const char *ns,
                                BSONObj& cmdObj,
                                BSONObjBuilder& result,
                                bool fromRepl );
        // For mongos
        static void execCommandClientBasic(Command* c,
                                           ClientBasic& client,
                                           int queryOptions,
                                           const char *ns,
                                           BSONObj& cmdObj,
                                           BSONObjBuilder& result,
                                           bool fromRepl );

        // Helper for setting errmsg and ok field in command result object.
        static void appendCommandStatus(BSONObjBuilder& result, bool ok, const std::string& errmsg);
        static void appendCommandStatus(BSONObjBuilder& result, const Status& status);

        // Set by command line.  Controls whether or not testing-only commands should be available.
        static int testCommandsEnabled;

    private:
        /**
         * Checks to see if the client is authorized to run the given command with the given
         * parameters on the given named database.
         *
         * fromRepl is true if this command is running as part of oplog application, which for
         * historic reasons has slightly different authorization semantics.  TODO(schwerin): Check
         * to see if this oddity can now be eliminated.
         *
         * Returns Status::OK() if the command is authorized.  Most likely returns
         * ErrorCodes::Unauthorized otherwise, but any return other than Status::OK implies not
         * authorized.
         */
        static Status _checkAuthorization(Command* c,
                                          ClientBasic* client,
                                          const std::string& dbname,
                                          const BSONObj& cmdObj,
                                          bool fromRepl);
    };

    /** A command that modifies metadata in some way. */
    class FileopsCommand : public Command {
      public:
        FileopsCommand(const char *name, bool webUI=false, const char *oldName=NULL) : Command(name, webUI, oldName) {}
        virtual LockType locktype() const { return WRITE; }
        virtual bool requiresSync() const { return true; }
        virtual bool needsTxn() const { return true; }
        virtual int txnFlags() const { return DB_SERIALIZABLE; }
        virtual OpSettings getOpSettings() const { return OpSettings(); }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        // These require some thought
        virtual bool logTheOp() = 0;
        virtual bool slaveOk() const { return false; }
    };

    /** A command that writes userdata to a Toku dictionary. */
    class ModifyCommand : public Command {
      public:
        ModifyCommand(const char *name, bool webUI=false, const char *oldName=NULL) : Command(name, webUI, oldName) {}
        virtual bool slaveOk() const { return false; }
        virtual LockType locktype() const { return READ; }
        virtual bool requiresSync() const { return true; }
        virtual bool needsTxn() const { return true; }
        virtual int txnFlags() const { return DB_SERIALIZABLE; }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual OpSettings getOpSettings() const { return OpSettings(); }
    };

    /** A command that only reads data out of a Toku dictionary. */
    class QueryCommand : public Command {
      public:
        QueryCommand(const char *name, bool webUI=false, const char *oldName=NULL) : Command(name, webUI, oldName) {}
        virtual bool slaveOk() const { return true; }
        virtual bool requiresShardedOperationScope() const { return false; }
        virtual LockType locktype() const { return READ; }
        virtual bool requiresSync() const { return false; }
        virtual bool needsTxn() const { return true; }
        virtual int txnFlags() const { return DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY; }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual OpSettings getOpSettings() const { return OpSettings().setBulkFetch(true); }
    };

    /** A command that doesn't read or write anything from Toku dictionaries, and just looks at some memory (like engine status) */
    class InformationCommand : public Command {
      public:
        InformationCommand(const char *name, bool webUI=false, const char *oldName=NULL) : Command(name, webUI, oldName) {}
        virtual bool slaveOk() const { return true; }
        virtual bool slaveOverrideOk() const { return true; }
        virtual bool requiresShardedOperationScope() const { return false; }
        virtual LockType locktype() const { return NONE; }
        virtual bool requiresSync() const { return false; }
        virtual bool needsTxn() const { return false; }
        virtual int txnFlags() const { return noTxnFlags(); }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual OpSettings getOpSettings() const { return OpSettings(); }
    };

    class WebInformationCommand : public InformationCommand {
      public:
        WebInformationCommand(const char *name, bool webUI=true, const char *oldName=NULL) : InformationCommand(name, webUI, oldName) {}
    };

    /** deprecated in tokuds */
    class DeprecatedCommand : public InformationCommand {
      public:
        DeprecatedCommand(const char *name, bool webUI=false, const char *oldName=NULL) : InformationCommand(name, false, oldName) {}
        // No auth required
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {}
        void help(stringstream &h) const { h << name << " is a deprecated command"; }
        bool run(const string&, BSONObj&, int, string &errmsg, BSONObjBuilder &res, bool) {
            stringstream ss;
            ss << name << " is a deprecated command, ignoring!";
            problem() << ss.str() << endl;
            errmsg = ss.str();
            res.append("errmsg", errmsg);
            res.append("ok", false);
            return false;
        }
    };

    // This will be registered instead of the real implementations of any commands that don't work
    // when auth is enabled.
    class NotWithAuthCmd : public InformationCommand {
    public:
        NotWithAuthCmd(const char* cmdName) : InformationCommand(cmdName) { }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {}
        virtual void help( stringstream &help ) const {
            help << name << " is not supported when running with authentication enabled";
        }
        virtual bool run(const string&,
                         BSONObj& cmdObj,
                         int,
                         string& errmsg,
                         BSONObjBuilder& result,
                         bool fromRepl) {
            errmsg = name + " is not supported when running with authentication enabled";
            return false;
        }
    };

    bool _runCommands(const char *ns, const BSONObj &cmdObj, BufBuilder &b, BSONObjBuilder& anObjBuilder, bool fromRepl, int queryOptions);

} // namespace mongo
