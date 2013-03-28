// dbcommands.cpp

/**
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
   See s/commnands_public.cpp for mongos.
*/

#include <time.h>

#include "mongo/pch.h"
#include "mongo/server.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/background.h"
#include "mongo/db/client.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/introspect.h"
#include "mongo/db/cursor.h"
#include "mongo/db/json.h"
#include "mongo/db/repl.h"
#include "mongo/db/repl_block.h"
#include "mongo/db/replutil.h"
#include "mongo/db/commands.h"
#include "mongo/db/db.h"
#include "mongo/db/instance.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/security.h"
#include "mongo/db/queryoptimizer.h"
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

    class CmdRepairDatabase : public Command {
    public:
        virtual bool logTheOp() {
            return false;
        }
        virtual bool slaveOk() const {
            return true;
        }
        virtual bool maintenanceMode() const { return true; }
        virtual void help( stringstream& help ) const {
            help << "repair database.  also compacts. note: slow.";
        }
        virtual LockType locktype() const { return WRITE; }
        // SERVER-4328 todo don't lock globally. currently syncDataAndTruncateJournal is being called within, and that requires a global lock i believe.
        virtual bool lockGlobally() const { return true; }
        CmdRepairDatabase() : Command("repairDatabase") {}
        bool run(const string& dbname , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            problem() << "repairDatabase is a deprecated command, ignoring!" << endl;
            return true;
        }
    } cmdRepairDatabase;

    class CollectionModCommand : public Command {
    public:
        CollectionModCommand() : Command( "collMod" ){}
        virtual bool slaveOk() const { return true; }
        virtual bool needsTxn() const { return false; }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual LockType locktype() const { return WRITE; }
        virtual bool logTheOp() { return true; }
        virtual void help( stringstream &help ) const {
            help << 
                "Sets collection options.\n"
                "Example: { collMod: 'foo', usePowerOf2Sizes:true } (deprecated)";
        }

        bool run(const string& dbname, BSONObj& jsobj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            errmsg = "CollectionModCommand is deprecated.";
            result.append( "errmsg" , errmsg );
            result.append( "ok", false );
            return false;
        }
    } collectionModCommand;

    /* convertToCapped seems to use this */
    class CmdCloneCollectionAsCapped : public Command {
    public:
        CmdCloneCollectionAsCapped() : Command( "cloneCollectionAsCapped" ) {}
        virtual bool slaveOk() const { return false; }
        virtual LockType locktype() const { return WRITE; }
        virtual void help( stringstream &help ) const {
            help << "{ cloneCollectionAsCapped:<fromName>, toCollection:<toName>, size:<sizeInBytes> } (deprecated)";
        }
        bool run(const string& dbname, BSONObj& jsobj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            errmsg = "CmdCloneCollectionAsCapped is deprecated.";
            result.append( "errmsg" , errmsg );
            result.append( "ok", false );
            return false;
        }
    } cmdCloneCollectionAsCapped;

    /* jan2010:
       Converts the given collection to a capped collection w/ the specified size.
       This command is not highly used, and is not currently supported with sharded
       environments.
       */
    class CmdConvertToCapped : public Command {
    public:
        CmdConvertToCapped() : Command( "convertToCapped" ) {}
        virtual bool slaveOk() const { return false; }
        virtual LockType locktype() const { return WRITE; }
        // calls renamecollection which does a global lock, so we must too:
        virtual bool lockGlobally() const { return true; }
        virtual void help( stringstream &help ) const {
            help << "{ convertToCapped:<fromCollectionName>, size:<sizeInBytes> } (deprecated)";
        }
        bool run(const string& dbname, BSONObj& jsobj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            errmsg = "CmdConvertToCapped is deprecated.";
            result.append( "errmsg" , errmsg );
            result.append( "ok", false );
            return false;
        } 
    } cmdConvertToCapped;

    /* For testing only, not for general use */
    class GodInsert : public Command {
    public:
        GodInsert() : Command( "godinsert" ) { }
        virtual bool adminOnly() const { return false; }
        virtual bool logTheOp() { return false; }
        virtual bool slaveOk() const { return true; }
        virtual LockType locktype() const { return NONE; }
        virtual bool requiresAuth() { return true; }
        virtual void help( stringstream &help ) const {
            help << "deprecated, does nothing";
        }
        virtual bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            return false;
        }
    } cmdGodInsert;

    class JournalLatencyTestCmd : public Command {
    public:
        JournalLatencyTestCmd() : Command( "journalLatencyTest" ) {}

        virtual bool slaveOk() const { return true; }
        virtual LockType locktype() const { return NONE; }
        virtual bool adminOnly() const { return true; }
        virtual void help(stringstream& h) const { h << "test how long to write and fsync to a test file in the journal/ directory"; }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            problem() << "journalLatencyTest is a deprecated command, doing nothing!" << endl;

            result.append("timeMillis", 0);
            result.append("timeMillisWithPrealloc", 0);

            return 1;
        }
    } journalLatencyTestCmd;

    class CleanCmd : public Command {
    public:
        CleanCmd() : Command( "clean" ) {}

        virtual bool slaveOk() const { return true; }
        virtual LockType locktype() const { return WRITE; }

        virtual void help(stringstream& h) const { h << "internal"; }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            string dropns = dbname + "." + cmdObj.firstElement().valuestrsafe();

            if ( !cmdLine.quiet )
                tlog() << "CMD: clean " << dropns << endl;

            NamespaceDetails *d = nsdetails(dropns.c_str());

            if ( ! d ) {
                errmsg = "ns not found";
                return 0;
            }

            problem() << "\"clean\" is a deprecated command, doing nothing!" << endl;

            result.append("ns", dropns.c_str());
            return 1;
        }

    } cleanCmd;

    namespace dur {
        boost::filesystem::path getJournalDir();
    }

    class ValidateCmd : public Command {
    public:
        ValidateCmd() : Command( "validate" ) {}

        virtual bool slaveOk() const {
            return true;
        }

        virtual void help(stringstream& h) const { h << "Validate contents of a namespace by scanning its data structures for correctness.  Slow.\n"
                                                        "Add full:true option to do a more thorough check"; }

        virtual LockType locktype() const { return READ; }
        //{ validate: "collectionnamewithoutthedbpart" [, scandata: <bool>] [, full: <bool> } */

        bool run(const string& dbname , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            string ns = dbname + "." + cmdObj.firstElement().valuestrsafe();
            NamespaceDetails * d = nsdetails( ns.c_str() );
            if ( !cmdLine.quiet )
                tlog() << "CMD: validate " << ns << endl;

            if ( ! d ) {
                errmsg = "ns not found";
                return false;
            }

            problem() << "validate is a deprecated command, assuming everything is ok." << endl;

            result.append( "ns", ns );
            result.appendBool("valid", true);
            return true;
        }
    } validateCmd;

    class LogRotateCmd : public Command {
    public:
        LogRotateCmd() : Command( "logRotate" ) {}
        virtual LockType locktype() const { return NONE; }
        virtual bool slaveOk() const { return true; }
        virtual bool adminOnly() const { return true; }
        virtual bool needsTxn() const { return false; }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual bool run(const string& ns, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            errmsg = "logRotate is deprecated.";
            result.append( "errmsg" , errmsg );
            result.append( "ok", false );
            return false;
        }

    } logRotateCmd;

}// namespace mongo
