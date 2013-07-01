// dbcommands_deprecated.cpp

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

#include "mongo/pch.h"

#include "mongo/db/commands.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/collection.h"
#include "mongo/db/opsettings.h"
#include "mongo/util/log.h"

namespace mongo {

    class CmdRepairDatabase : public DeprecatedCommand {
      public:
        CmdRepairDatabase() : DeprecatedCommand("repairDatabase") {}
    } cmdRepairDatabase;

    class CompactCmd : public DeprecatedCommand {
      public:
        CompactCmd() : DeprecatedCommand("compact") {}
    } compactCmd;

    class CollectionModCommand : public DeprecatedCommand {
      public:
        CollectionModCommand() : DeprecatedCommand("collMod") {}
    } collectionModCommand;

    class CmdCloneCollectionAsCapped : public DeprecatedCommand {
      public:
        CmdCloneCollectionAsCapped() : DeprecatedCommand("cloneCollectionAsCapped") {}
    } cmdCloneCollectionAsCapped;

    /* jan2010:
       Converts the given collection to a capped collection w/ the specified size.
       This command is not highly used, and is not currently supported with sharded
       environments.
       */
    class CmdConvertToCapped : public DeprecatedCommand {
      public:
        CmdConvertToCapped() : DeprecatedCommand("convertToCapped") {}
    } cmdConvertToCapped;

    class GodInsert : public DeprecatedCommand {
      public:
        GodInsert() : DeprecatedCommand("godinsert") {}
    } cmdGodInsert;

    class JournalLatencyTestCmd : public DeprecatedCommand {
      public:
        JournalLatencyTestCmd() : DeprecatedCommand("journalLatencyTest") {}
    } journalLatencyTestCmd;

    class CleanCmd : public DeprecatedCommand {
      public:
        CleanCmd() : DeprecatedCommand("clean") {}
    } cleanCmd;

    // We (probably) don't deprecate this fully because we want callers
    // to always get back a successful result (and so they think the
    // collection is in an okay condition).
    class ValidateCmd : public Command {
    public:
        ValidateCmd() : Command( "validate" ) {}

        virtual bool slaveOk() const {
            return true;
        }

        virtual void help(stringstream& h) const { h << "Validate contents of a namespace by scanning its data structures for correctness.  Slow.\n"
                                                        "Add full:true option to do a more thorough check"; }

        virtual bool requiresShardedOperationScope() const { return false; }
        virtual LockType locktype() const { return READ; }
        //{ validate: "collectionnamewithoutthedbpart" [, scandata: <bool>] [, full: <bool> } */
        virtual bool requiresSync() const { return false; }
        virtual bool needsTxn() const { return false; }
        virtual int txnFlags() const { return noTxnFlags(); }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual OpSettings getOpSettings() const { return OpSettings(); }
        // No auth required
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {}

        bool run(const string& dbname , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            string ns = dbname + "." + cmdObj.firstElement().valuestrsafe();
            Collection *cl = getCollection(ns);
            if ( !cmdLine.quiet )
                MONGO_TLOG(0) << "CMD: validate " << ns << endl;

            if ( ! cl ) {
                errmsg = "ns not found";
                return false;
            }

            problem() << "validate is a deprecated command, assuming everything is ok." << endl;

            result.append( "ns", ns );
            result.appendBool("valid", true);
            return true;
        }
    } validateCmd;

    class ApplyOpsCmd : public DeprecatedCommand {
      public:
        ApplyOpsCmd() : DeprecatedCommand("applyOps") {}
    } applyOpsCmd;

    class CmdGetOpTime : public DeprecatedCommand {
      public:
        CmdGetOpTime() : DeprecatedCommand("getoptime") {}
    } cmdgetoptime;

}// namespace mongo
