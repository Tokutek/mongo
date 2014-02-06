/* hashcmd.cpp
 *
 * Defines a shell command for hashing a BSONElement value
 */


/**
*    Copyright (C) 2012 10gen Inc.
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

#include "mongo/base/init.h"
#include "mongo/db/commands.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/repl/rs_config.h"

namespace mongo {

    class CmdTestHooks : public InformationCommand {
    public:
        CmdTestHooks() : InformationCommand("_testHooks") {};
        virtual void help( stringstream& help ) const {
            help << "internal command used for testing";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {}

        bool run(
            const string& db,
            BSONObj& cmdObj,
            int options, string& errmsg,
            BSONObjBuilder& result,
            bool fromRepl = false )
        {
            if (cmdObj.hasField("keepOplogAlive")) {
                uint64_t val = cmdObj["keepOplogAlive"].numberLong();
                if (theReplSet) {
                    theReplSet->setKeepOplogAlivePeriod(val);
                }
            }

            return true;
        }
    };

    MONGO_INITIALIZER(RegisterTestHooksCmd)(InitializerContext* context) {
        if (Command::testCommandsEnabled) {
            // Leaked intentionally: a Command registers itself when constructed.
            new CmdTestHooks();
        }
        return Status::OK();
    }

    class CmdChangeOplogVersion : public InformationCommand {
    public:
        CmdChangeOplogVersion() : InformationCommand("_changeOplogVersion") {};
        virtual void help( stringstream& help ) const {
            help << "internal command used for testing";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {}

        bool run(
            const string& db,
            BSONObj& cmdObj,
            int options, string& errmsg,
            BSONObjBuilder& result,
            bool fromRepl = false )
        {
            if (cmdObj.hasField("reset")) {
                ReplSetConfig::OPLOG_VERSION = ReplSetConfig::OPLOG_VERSION_CURRENT;
            }
            else {
                ReplSetConfig::OPLOG_VERSION = ReplSetConfig::OPLOG_VERSION_TEST;
            }
            return true;
        }
    };

    MONGO_INITIALIZER(RegisterChangeOplogVersionCmd)(InitializerContext* context) {
        if (Command::testCommandsEnabled) {
            // Leaked intentionally: a Command registers itself when constructed.
            new CmdChangeOplogVersion();
        }
        return Status::OK();
    }


    class CmdChangePartitionCreateTime : public InformationCommand {
    public:
        CmdChangePartitionCreateTime() : InformationCommand("_changePartitionCreateTime") {};
        virtual void help( stringstream& help ) const {
            help << "internal command used for testing";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {}

        bool run(
            const string& db,
            BSONObj& cmdObj,
            int options, string& errmsg,
            BSONObjBuilder& result,
            bool fromRepl = false )
        {
            LOCK_REASON(lockReason, "changing create time of a partition in test hook");
            Client::ReadContext ctx(db, lockReason);
            Client::Transaction transaction(DB_SERIALIZABLE);
            string coll = cmdObj[ "_changePartitionCreateTime" ].valuestrsafe();
            uassert( 17263, "_changePartitionCreateTime must specify a collection", !coll.empty() );
            string ns = db + "." + coll;
            Collection *cl = getCollection( ns );
            uassert( 17264, "no such collection", cl );
            uassert( 17265, "collection must be partitioned", cl->isPartitioned() );

            // change the create time for a partition at a certain index
            PartitionedCollection* pc = cl->as<PartitionedCollection>();
            uint64_t index = cmdObj["index"].numberLong();            
            BSONObj refMeta = pc->getPartitionMetadata(index);
            BSONObjBuilder bbb;
            cloneBSONWithFieldChanged(bbb, refMeta, cmdObj["createTime"]);
            pc->updatePartitionMetadata(index, bbb.done(), false);
            transaction.commit();
            return true;
        }
    };

    MONGO_INITIALIZER(RegisterChangePartitionCreateTimeCmd)(InitializerContext* context) {
        if (Command::testCommandsEnabled) {
            // Leaked intentionally: a Command registers itself when constructed.
            new CmdChangePartitionCreateTime();
        }
        return Status::OK();
    }
}
