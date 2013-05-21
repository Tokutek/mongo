/**
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

#include "pch.h"

#include "mongo/db/commands.h"
#include <db.h>
#include "mongo/db/client.h"

namespace mongo {

    class BeginTransactionCmd : public InformationCommand {
    public:
        virtual bool adminOnly() const { return false; }
        virtual bool requiresAuth() { return true; }
        virtual LockType locktype() const { return READ; }
        virtual void help( stringstream& help ) const {
            help << "begin transaction\n"
                "Create a transaction for multiple statements.\n"
                "{ beginTransaction, [isolation : ]  }\n"
                " Possible values for isolation: serializable, mvcc (default), readUncommitted \n";
        }
        BeginTransactionCmd() : InformationCommand("beginTransaction") {}

        virtual bool run(const string& db, 
                         BSONObj& cmdObj, 
                         int, 
                         string& errmsg, 
                         BSONObjBuilder& result, 
                         bool fromRepl) 
        {
            uint32_t iso_flags = 0;
            BSONElement isoBSON = cmdObj["isolation"];
            if (isoBSON.eoo()) {
                iso_flags = DB_TXN_SNAPSHOT;
            }
            else if (isoBSON.type() != String) {
                uasserted(16738, "invalid isolation passed in");
            }
            else {
                string iso = isoBSON.String();
                if (iso == "serializable") {
                    iso_flags = 0;
                }
                else if (iso == "mvcc") {
                    iso_flags = DB_TXN_SNAPSHOT;
                }
                else if (iso == "readUncommitted") {
                    iso_flags = DB_READ_UNCOMMITTED;
                }
                else {
                    uasserted(16739, "invalid isolation passed in");
                }
            }

            uassert(16787, "transaction already exists", !cc().hasTxn());
            cc().beginClientTxn(iso_flags);
            result.append("status", "transaction began");
            return true;
        }
    } beginTransactionCmd;

    class CommitTransactionCmd : public InformationCommand {
    public:
        virtual bool adminOnly() const { return false; }
        virtual bool requiresAuth() { return true; }
        virtual LockType locktype() const { return READ; }
        virtual void help( stringstream& help ) const {
            help << "commit transaction\n"
                "If running a multi statement transaction, commit transaction, no-op otherwise .\n"
                "{ commitTransaction }";
        }
        CommitTransactionCmd() : InformationCommand("commitTransaction") {}

        virtual bool run(const string& db, 
                         BSONObj& cmdObj, 
                         int, 
                         string& errmsg, 
                         BSONObjBuilder& result, 
                         bool fromRepl) 
        {
            uassert(16788, "no transaction exists to be committed", cc().hasTxn());
            result.append("status", "transaction committed");
            cc().commitTopTxn();
            // after committing txn, there should be 
            // no txn left on stack, having a dassert to verify
            dassert(!cc().hasTxn());
            return true;
        }
    } commitTransactionCmd;

    class RollbackTransactionCmd : public InformationCommand {
    public:
        virtual bool adminOnly() const { return false; }
        virtual bool requiresAuth() { return true; }
        virtual LockType locktype() const { return READ; }
        virtual void help( stringstream& help ) const {
            help << "rollback transaction\n"
                "If running a multi statement transaction, rollback transaction, no-op otherwise .\n"
                "{ rollbackTransaction }";
        }
        RollbackTransactionCmd() : InformationCommand("rollbackTransaction") {}

        virtual bool run(const string& db, 
                         BSONObj& cmdObj, 
                         int, 
                         string& errmsg, 
                         BSONObjBuilder& result, 
                         bool fromRepl) 
        {
            uassert(16789, "no transaction exists to be rolled back", cc().hasTxn());
            result.append("status", "transaction rolled back");
            cc().abortTopTxn();
            // after committing txn, there should be 
            // no txn left on stack, having a dassert to verify
            dassert(!cc().hasTxn());
            return true;
        }
    } rollbackTransactionCmd;
}
