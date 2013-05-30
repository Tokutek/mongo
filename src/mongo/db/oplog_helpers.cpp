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

#include "mongo/pch.h"
#include "oplog_helpers.h"
#include "txn_context.h"
#include "repl_block.h"
#include "stats/counters.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/repl/rs.h"


#define KEY_STR_OP_NAME "op"
#define KEY_STR_NS "ns"
#define KEY_STR_ROW "o"
#define KEY_STR_OLD_ROW "o"
#define KEY_STR_NEW_ROW "o2"
#define KEY_STR_PK "pk"
#define KEY_STR_COMMENT "o"
#define KEY_STR_MIGRATE "fromMigrate"

namespace mongo {
namespace OpLogHelpers{

    static inline void appendOpType(const char *opstr, BSONObjBuilder* b) {
        b->append(KEY_STR_OP_NAME, opstr);
    }
    static inline void appendNsStr(const char *ns, BSONObjBuilder* b) {
        b->append(KEY_STR_NS, ns);
    }
    static inline void appendMigrate(bool fromMigrate, BSONObjBuilder* b) {
        if (fromMigrate) {
            b->append(KEY_STR_MIGRATE, true);
        }
    }

    static inline bool isLocalNs(const char* ns) {
        return (strncmp(ns, "local.", 6) == 0);
    }
    
    void logComment(BSONObj comment, TxnContext* txn) {
        if (logTxnOpsForReplication()) {
            BSONObjBuilder b;
            appendOpType(OP_STR_COMMENT, &b);
            b.append(KEY_STR_COMMENT, comment);
            txn->logOpForReplication(b.obj());
        }
    }
    
    void logInsert(const char* ns, BSONObj row, TxnContext* txn) {
        bool logForSharding = shouldLogTxnOpForSharding(OP_STR_INSERT, ns, row);
        if (logTxnOpsForReplication() || logForSharding) {
            BSONObjBuilder b;
            if (isLocalNs(ns)) {
                return;
            }

            appendOpType(OP_STR_INSERT, &b);
            appendNsStr(ns, &b);
            b.append(KEY_STR_ROW, row);
            BSONObj logObj = b.obj();
            if (logTxnOpsForReplication()) {
                txn->logOpForReplication(logObj);
            }
            if (logForSharding) {
                txn->logOpForSharding(logObj);
            }
        }
    }

    void logInsertForCapped(
        const char* ns, 
        BSONObj pk, 
        BSONObj row, 
        TxnContext* txn
        ) 
    {
        if (logTxnOpsForReplication()) {
            BSONObjBuilder b;
            if (isLocalNs(ns)) {
                return;
            }

            appendOpType(OP_STR_CAPPED_INSERT, &b);
            appendNsStr(ns, &b);
            b.append(KEY_STR_PK, pk);
            b.append(KEY_STR_ROW, row);
            txn->logOpForReplication(b.obj());
        }
    }

    void logUpdate(
        const char* ns,
        const BSONObj& pk,
        const BSONObj& oldRow, 
        const BSONObj& newRow,
        bool fromMigrate,
        TxnContext* txn
        ) 
    {
        bool logForSharding = !fromMigrate && shouldLogTxnUpdateOpForSharding(OP_STR_UPDATE, ns, oldRow, newRow);
        if (logTxnOpsForReplication() || logForSharding) {
            BSONObjBuilder b;
            if (isLocalNs(ns)) {
                return;
            }

            appendOpType(OP_STR_UPDATE, &b);
            appendNsStr(ns, &b);
            appendMigrate(fromMigrate, &b);
            b.append(KEY_STR_PK, pk);
            b.append(KEY_STR_OLD_ROW, oldRow);
            b.append(KEY_STR_NEW_ROW, newRow);
            BSONObj logObj = b.obj();
            if (logTxnOpsForReplication()) {
                txn->logOpForReplication(logObj);
            }
            if (logForSharding) {
                txn->logOpForSharding(logObj);
            }
        }
    }

    void logDelete(const char* ns, BSONObj row, bool fromMigrate, TxnContext* txn) {
        bool logForSharding = !fromMigrate && shouldLogTxnOpForSharding(OP_STR_DELETE, ns, row);
        if (logTxnOpsForReplication() || logForSharding) {
            BSONObjBuilder b;
            if (isLocalNs(ns)) {
                return;
            }

            appendOpType(OP_STR_DELETE, &b);
            appendNsStr(ns, &b);
            appendMigrate(fromMigrate, &b);
            b.append(KEY_STR_ROW, row);
            BSONObj logObj = b.obj();
            if (logTxnOpsForReplication()) {
                txn->logOpForReplication(logObj);
            }
            if (logForSharding) {
                txn->logOpForSharding(logObj);
            }
        }
    }

    void logDeleteForCapped(
        const char* ns, 
        BSONObj pk,
        BSONObj row, 
        TxnContext* txn
        ) 
    {
        if (logTxnOpsForReplication()) {
            BSONObjBuilder b;
            if (isLocalNs(ns)) {
                return;
            }

            appendOpType(OP_STR_CAPPED_DELETE, &b);
            appendNsStr(ns, &b);
            b.append(KEY_STR_PK, pk);
            b.append(KEY_STR_ROW, row);
            txn->logOpForReplication(b.obj());
        }
    }

    void logCommand(const char* ns, BSONObj row, TxnContext* txn) {
        // We do not need to log for sharding because commands are only logged right now if they
        // take a write lock, and we have a read lock the whole time we're logging things for
        // sharding.  TODO: If this changes, we need to start logging commands.
        if (logTxnOpsForReplication()) {
            BSONObjBuilder b;
            if (isLocalNs(ns)) {
                return;
            }

            appendOpType(OP_STR_COMMAND, &b);
            appendNsStr(ns, &b);
            b.append(KEY_STR_ROW, row);
            txn->logOpForReplication(b.obj());
        }
    }

    static void runNonSystemInsertFromOplogWithLock(
        const char* ns, 
        BSONObj row
        ) 
    {
        NamespaceDetails* nsd = nsdetails(ns);
        NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get(ns);
        // overwrite set to true because we are running on a secondary
        insertOneObject(nsd, nsdt, row, NamespaceDetails::NO_UNIQUE_CHECKS);
    }
    static void runInsertFromOplog(const char* ns, BSONObj op) {
        BSONObj row = op[KEY_STR_ROW].Obj();
        // handle add index case
        if (mongoutils::str::endsWith(ns, ".system.indexes")) {
            // do not build the index if the user has disabled
            if (theReplSet->buildIndexes()) {
                Client::WriteContext ctx(ns);
                NamespaceDetails* nsd = nsdetails(ns);
                NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get(ns);
                BSONObj key = row["key"].Obj();
                const string &coll = row["ns"].String();
                NamespaceDetails* collNsd = nsdetails(coll.c_str());
                int i = collNsd->findIndexByKeyPattern(key);
                if (i >= 0) {
                    // the index already exists, so this is a no-op
                    // Note that for create index and drop index, we
                    // are tolerant of the fact that the operation may
                    // have already been done
                    return;
                } else {
                    collNsd->createIndex(row);
                }
                // overwrite set to true because we are running on a secondary
                insertOneObject(nsd, nsdt, row, NamespaceDetails::NO_UNIQUE_CHECKS);
            }
        }
        else {
            try {
                Client::ReadContext ctx(ns);
                runNonSystemInsertFromOplogWithLock(ns, row);
            }
            catch (RetryWithWriteLock &e) {
                Client::WriteContext ctx(ns);
                runNonSystemInsertFromOplogWithLock(ns, row);
            }
        }
    }

    static void runCappedInsertFromOplogWithLock(
        const char* ns, 
        BSONObj& pk,
        BSONObj& row
        ) 
    {
        NamespaceDetails* nsd = NULL;
        NamespaceDetailsTransient *nsdt = NULL;
        nsd = nsdetails(ns);
        nsdt = &NamespaceDetailsTransient::get(ns);
        // overwrite set to true because we are running on a secondary
        nsd->insertObjectIntoCappedWithPK(pk, row, NamespaceDetails::NO_UNIQUE_CHECKS);
        if (nsdt != NULL) {
            nsdt->notifyOfWriteOp();
        }
    }
    
    static void runCappedInsertFromOplog(const char* ns, BSONObj op) {
        BSONObj pk = op[KEY_STR_PK].Obj();
        BSONObj row = op[KEY_STR_ROW].Obj();
        try {
            Client::ReadContext ctx(ns);
            runCappedInsertFromOplogWithLock(ns, pk, row);
        }
        catch (RetryWithWriteLock &e) {
            Client::WriteContext ctx(ns);
            runCappedInsertFromOplogWithLock(ns, pk, row);
        }
    }

    static void runDeleteFromOplogWithLock(const char* ns, BSONObj op) {
        NamespaceDetails* nsd = nsdetails(ns);
        NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get(ns);
        BSONObj row = op[KEY_STR_ROW].Obj();
        BSONObj pk = row["_id"].wrap("");
        deleteOneObject(nsd, nsdt, pk, row);
    }

    static void runDeleteFromOplog(const char* ns, BSONObj op) {
        try {
            Client::ReadContext ctx(ns);
            runDeleteFromOplogWithLock(ns, op);
        }
        catch (RetryWithWriteLock &e) {
            Client::WriteContext ctx(ns);
            runDeleteFromOplogWithLock(ns, op);
        }        
    }

    static void runCappedDeleteFromOplogWithLock(const char* ns, BSONObj op) {
        NamespaceDetails* nsd = nsdetails(ns);
        NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get(ns);
        BSONObj row = op[KEY_STR_ROW].Obj();
        BSONObj pk = op[KEY_STR_PK].Obj();

        nsd->deleteObjectIntoCappedWithPK(pk, row);
        if (nsdt != NULL) {
            nsdt->notifyOfWriteOp();
        }
    }
    
    static void runCappedDeleteFromOplog(const char* ns, BSONObj op) {
        try {
            Client::ReadContext ctx(ns);
            runCappedDeleteFromOplogWithLock(ns, op);
        }
        catch (RetryWithWriteLock &e) {
            Client::WriteContext ctx(ns);
            runCappedDeleteFromOplogWithLock(ns, op);
        }
    }

    static void runUpdateFromOplogWithLock(const char* ns, BSONObj op, bool isRollback) {
        NamespaceDetails* nsd = nsdetails(ns);
        NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get(ns);
        const char *names[] = {
            KEY_STR_PK,
            KEY_STR_OLD_ROW, 
            KEY_STR_NEW_ROW
            };
        BSONElement fields[3];
        op.getFields(3, names, fields);
        BSONObj pk = fields[0].Obj();
        BSONObj oldRow = fields[1].Obj();
        BSONObj newRow = fields[2].Obj();
        // note the only difference between these two cases is
        // what is passed as the before image, and what is passed
        // as after. In normal replication, we replace oldRow with newRow.
        // In rollback, we replace newRow with oldRow
        if (isRollback) {
            // if this is a rollback, then the newRow is what is in the
            // collections, that we want to replace with oldRow
            updateOneObject(nsd, nsdt, pk, newRow, oldRow, NULL);
        }
        else {
            // normal replication case
            updateOneObject(nsd, nsdt, pk, oldRow, newRow, NULL);
        }
    }
    static void runUpdateFromOplog(const char* ns, BSONObj op, bool isRollback) {
        try {
            Client::ReadContext ctx(ns);
            runUpdateFromOplogWithLock(ns, op, isRollback);
        }
        catch (RetryWithWriteLock &e) {
            Client::WriteContext ctx(ns);
            runUpdateFromOplogWithLock(ns, op, isRollback);
        }        
    }

    static void runCommandFromOplog(const char* ns, BSONObj op) {
        BufBuilder bb;
        BSONObjBuilder ob;
        BSONObj command = op[KEY_STR_ROW].embeddedObject();
        // locking ought to be taken care of inside the command
        // possibly redo how this works.
        _runCommands(ns, command, bb, ob, true, 0);
    }

    static void rollbackCommandFromOplog(const char* ns, BSONObj op) {
        BSONObj command = op[KEY_STR_ROW].embeddedObject();
        log() << "Cannot rollback command " << op << rsLog;
        throw RollbackOplogException(str::stream() << "Could not rollback command " << command << " on ns " << ns);
    }
    
    void applyOperationFromOplog(const BSONObj& op) {
        LOG(6) << "applying op: " << op << endl;
        OpCounters* opCounters = &replOpCounters;
        const char *names[] = { 
            KEY_STR_NS, 
            KEY_STR_OP_NAME
            };
        BSONElement fields[2];
        op.getFields(2, names, fields);
        const char* ns = fields[0].valuestrsafe();
        const char* opType = fields[1].valuestrsafe();
        if (strcmp(opType, OP_STR_INSERT) == 0) {
            opCounters->gotInsert();
            runInsertFromOplog(ns, op);
        }
        else if (strcmp(opType, OP_STR_UPDATE) == 0) {
            opCounters->gotUpdate();
            runUpdateFromOplog(ns, op, false);
        }
        else if (strcmp(opType, OP_STR_DELETE) == 0) {
            opCounters->gotDelete();
            runDeleteFromOplog(ns, op);
        }
        else if (strcmp(opType, OP_STR_COMMAND) == 0) {
            opCounters->gotCommand();
            runCommandFromOplog(ns, op);
        }
        else if (strcmp(opType, OP_STR_COMMENT) == 0) {
            // no-op
        }
        else if (strcmp(opType, OP_STR_CAPPED_INSERT) == 0) {
            opCounters->gotInsert();
            runCappedInsertFromOplog(ns, op);
        }
        else if (strcmp(opType, OP_STR_CAPPED_DELETE) == 0) {
            opCounters->gotDelete();
            runCappedDeleteFromOplog(ns, op);
        }
        else {
            throw MsgAssertionException( 14825 , ErrorMsg("error in applyOperation : unknown opType ", *opType) );
        }
    }

    static void runRollbackInsertFromOplog(const char* ns, BSONObj op) {
        // handle add index case
        if (mongoutils::str::endsWith(ns, ".system.indexes")) {
            throw RollbackOplogException(str::stream() << "Not rolling back an add index on " << ns << ". Op: " << op.toString(false, true));
        }
        else {
            // the rollback of a normal insert is to do the delete
            runDeleteFromOplog(ns, op);
        }
    }

    void rollbackOperationFromOplog(const BSONObj& op) {
        LOG(6) << "rolling back op: " << op << endl;
        const char *names[] = { 
            KEY_STR_NS, 
            KEY_STR_OP_NAME
            };
        BSONElement fields[2];
        op.getFields(2, names, fields);
        const char* ns = fields[0].valuestrsafe();
        const char* opType = fields[1].valuestrsafe();
        if (strcmp(opType, OP_STR_INSERT) == 0) {
            runRollbackInsertFromOplog(ns, op);
        }
        else if (strcmp(opType, OP_STR_UPDATE) == 0) {
            runUpdateFromOplog(ns, op, true);
        }
        else if (strcmp(opType, OP_STR_DELETE) == 0) {
            // the rollback of a delete is to do the insert
            runInsertFromOplog(ns, op);
        }
        else if (strcmp(opType, OP_STR_COMMAND) == 0) {
            rollbackCommandFromOplog(ns, op);
        }
        else if (strcmp(opType, OP_STR_COMMENT) == 0) {
            // no-op
        }
        else if (strcmp(opType, OP_STR_CAPPED_INSERT) == 0) {
            runCappedDeleteFromOplog(ns, op);
        }
        else if (strcmp(opType, OP_STR_CAPPED_DELETE) == 0) {
            runCappedInsertFromOplog(ns, op);
        }
        else {
            throw MsgAssertionException( 16795 , ErrorMsg("error in applyOperation : unknown opType ", *opType) );
        }
    }
} // namespace OpLogHelpers
} // namespace mongo
