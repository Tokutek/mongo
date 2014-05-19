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
#include "mongo/db/collection.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/txn_context.h"
#include "mongo/db/repl_block.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/update_internal.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/relock.h"

// BSON fields for oplog entries
static const char *KEY_STR_OP_NAME = "op";
static const char *KEY_STR_NS = "ns";
static const char *KEY_STR_ROW = "o";
static const char *KEY_STR_OLD_ROW = "o";
static const char *KEY_STR_NEW_ROW = "o2";
static const char *KEY_STR_MODS = "m";
static const char *KEY_STR_PK = "pk";
static const char *KEY_STR_COMMENT = "o";
static const char *KEY_STR_MIGRATE = "fromMigrate";

// values for types of operations in oplog
static const char OP_STR_INSERT[] = "i"; // normal insert
static const char OP_STR_CAPPED_INSERT[] = "ci"; // insert into capped collection
static const char OP_STR_UPDATE[] = "u"; // normal update with full pre-image and full post-image
static const char OP_STR_UPDATE_ROW_WITH_MOD[] = "ur"; // update with full pre-image and mods to generate post-image
static const char OP_STR_DELETE[] = "d"; // delete with full pre-image
static const char OP_STR_CAPPED_DELETE[] = "cd"; // delete from capped collection
static const char OP_STR_COMMENT[] = "n"; // a no-op
static const char OP_STR_COMMAND[] = "c"; // command

namespace mongo {

    namespace OplogHelpers {
        bool shouldLogOpForSharding(const char *opstr) {
            return mongoutils::str::equals(opstr, OP_STR_INSERT) ||
                mongoutils::str::equals(opstr, OP_STR_DELETE) ||
                mongoutils::str::equals(opstr, OP_STR_UPDATE) ||
                mongoutils::str::equals(opstr, OP_STR_UPDATE_ROW_WITH_MOD);
        }

        bool invalidOpForSharding(const char *opstr) {
            return mongoutils::str::equals(opstr, OP_STR_CAPPED_INSERT) ||
                mongoutils::str::equals(opstr, OP_STR_CAPPED_DELETE);
        }

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

        static inline bool isLocalNs(const char *ns) {
            return (strncmp(ns, "local.", 6) == 0);
        }
        
        void logComment(const BSONObj &comment) {
            if (logTxnOpsForReplication()) {
                BSONObjBuilder b;
                appendOpType(OP_STR_COMMENT, &b);
                b.append(KEY_STR_COMMENT, comment);
                cc().txn().logOpForReplication(b.obj());
            }
        }
        
        void logInsert(const char *ns, const BSONObj &row, bool fromMigrate) {
            bool logForSharding = !fromMigrate &&
                                  shouldLogTxnOpForSharding(OP_STR_INSERT, ns, row);
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
                    cc().txn().logOpForReplication(logObj);
                }
                if (logForSharding) {
                    cc().txn().logOpForSharding(logObj);
                }
            }
        }

        void logInsertForCapped(const char *ns, const BSONObj &pk, const BSONObj &row) {
            if (logTxnOpsForReplication()) {
                BSONObjBuilder b;
                if (isLocalNs(ns)) {
                    return;
                }

                appendOpType(OP_STR_CAPPED_INSERT, &b);
                appendNsStr(ns, &b);
                b.append(KEY_STR_PK, pk);
                b.append(KEY_STR_ROW, row);
                cc().txn().logOpForReplication(b.obj());
            }
        }

        void logUpdate(const char *ns, const BSONObj &pk,
                       const BSONObj &oldObj, const BSONObj &newObj,
                       bool fromMigrate) {
            bool logForSharding = !fromMigrate &&
                shouldLogTxnUpdateOpForSharding(OP_STR_UPDATE, ns, oldObj);
            if (logTxnOpsForReplication() || logForSharding) {
                BSONObjBuilder b;
                if (isLocalNs(ns)) {
                    return;
                }

                appendOpType(OP_STR_UPDATE, &b);
                appendNsStr(ns, &b);
                appendMigrate(fromMigrate, &b);
                b.append(KEY_STR_PK, pk);
                b.append(KEY_STR_OLD_ROW, oldObj);
                // otherwise we just log the full old row and full new row
                verify(!newObj.isEmpty());
                b.append(KEY_STR_NEW_ROW, newObj);
                BSONObj logObj = b.obj();
                if (logTxnOpsForReplication()) {
                    cc().txn().logOpForReplication(logObj);
                }
                if (logForSharding) {
                    cc().txn().logOpForSharding(logObj);
                }
            }
        }

        void logUpdateModsWithRow(
            const char *ns,
            const BSONObj &pk,
            const BSONObj &oldObj,
            const BSONObj &updateobj,
            bool fromMigrate
            ) 
        {
            bool logForSharding = !fromMigrate &&
                shouldLogTxnUpdateOpForSharding(OP_STR_UPDATE, ns, oldObj);
            if (logTxnOpsForReplication() || logForSharding) {
                BSONObjBuilder b;
                if (isLocalNs(ns)) {
                    return;
                }

                appendOpType(OP_STR_UPDATE_ROW_WITH_MOD, &b);
                appendNsStr(ns, &b);
                appendMigrate(fromMigrate, &b);
                b.append(KEY_STR_PK, pk);
                b.append(KEY_STR_OLD_ROW, oldObj);
                b.append(KEY_STR_MODS, updateobj);
                BSONObj logObj = b.obj();
                if (logTxnOpsForReplication()) {
                    cc().txn().logOpForReplication(logObj);
                }
                if (logForSharding) {
                    cc().txn().logOpForSharding(logObj);
                }
            }
        }

        void logDelete(const char *ns, const BSONObj &row, bool fromMigrate) {
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
                    cc().txn().logOpForReplication(logObj);
                }
                if (logForSharding) {
                    cc().txn().logOpForSharding(logObj);
                }
            }
        }

        void logDeleteForCapped(const char *ns, const BSONObj &pk, const BSONObj &row) {
            if (logTxnOpsForReplication()) {
                BSONObjBuilder b;
                if (isLocalNs(ns)) {
                    return;
                }

                appendOpType(OP_STR_CAPPED_DELETE, &b);
                appendNsStr(ns, &b);
                b.append(KEY_STR_PK, pk);
                b.append(KEY_STR_ROW, row);
                cc().txn().logOpForReplication(b.obj());
            }
        }

        void logCommand(const char *ns, const BSONObj &row) {
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
                cc().txn().logOpForReplication(b.obj());
            }
        }

        void logUnsupportedOperation(const char *ns) {
            if (logTxnOpsForReplication()) {
                if (isLocalNs(ns)) {
                    return;
                }
                uasserted(17293, "The operation is not supported for replication");
            }
        }
        
        static void runColdIndexFromOplog(const char *ns, const BSONObj &row) {
            LOCK_REASON(lockReason, "repl: cold index build");
            Client::WriteContext ctx(ns, lockReason);
            Collection *sysCl = getCollection(ns);
            const string &coll = row["ns"].String();

            Collection *cl = getCollection(coll);
            const bool ok = cl->ensureIndex(row);
            if (!ok) {
                // the index already exists, so this is a no-op
                // Note that for create index and drop index, we
                // are tolerant of the fact that the operation may
                // have already been done
                return;
            }
            BSONObj obj = row;
            insertOneObject(sysCl, obj, Collection::NO_UNIQUE_CHECKS);
        }

        static void runHotIndexFromOplog(const char *ns, const BSONObj &row) {
            // The context and lock must outlive the indexer so that
            // the indexer destructor gets called in a write locked.
            // These MUST NOT be reordered, the context must destruct
            // after the indexer.
            LOCK_REASON(lockReason, "repl: hot index build");
            scoped_ptr<Lock::DBWrite> lk(new Lock::DBWrite(ns, lockReason));
            shared_ptr<CollectionIndexer> indexer;
            const string &coll = row["ns"].String();

            {
                Client::Context ctx(ns);
                Collection *sysCl = getCollection(ns);

                Collection *cl = getCollection(coll);
                if (cl->findIndexByKeyPattern(row["key"].Obj()) >= 0) {
                    // the index already exists, so this is a no-op
                    // Note that for create index and drop index, we
                    // are tolerant of the fact that the operation may
                    // have already been done
                    return;
                }
                BSONObj obj = row;
                insertOneObject(sysCl, obj, Collection::NO_UNIQUE_CHECKS);
                indexer = cl->newHotIndexer(row);
                indexer->prepare();
                addToNamespacesCatalog(IndexDetails::indexNamespace(coll, row["name"].String()));
            }

            {
                Lock::DBWrite::Downgrade dg(lk);
                Client::Context ctx(ns);
                indexer->build();
            }

            {
                Client::Context ctx(ns);
                indexer->commit();
                Collection *cl = getCollection(coll);
                verify(cl);
                cl->noteIndexBuilt();
            }
        }

        static void runNonSystemInsertFromOplogWithLock(
            const char *ns, 
            const BSONObj &row
            )
        {
            Collection *cl = getCollection(ns);
            
            // overwrite set to true because we are running on a secondary
            const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
            BSONObj obj = row;
            insertOneObject(cl, obj, flags);
        }

        static void runInsertFromOplog(const char *ns, const BSONObj &op) {
            const BSONObj row = op[KEY_STR_ROW].Obj();
            // handle add index case
            if (nsToCollectionSubstring(ns) == "system.indexes") {
                // do not build the index if the user has disabled
                if (theReplSet->buildIndexes()) {
                    if (row["background"].trueValue()) {
                        runHotIndexFromOplog(ns, row);
                    } else {
                        runColdIndexFromOplog(ns, row);
                    }
                }
            }
            else {
                try {
                    LOCK_REASON(lockReason, "repl: applying insert");
                    Client::ReadContext ctx(ns, lockReason);
                    runNonSystemInsertFromOplogWithLock(ns, row);
                }
                catch (RetryWithWriteLock &e) {
                    LOCK_REASON(lockReason, "repl: applying insert with write lock");
                    Client::WriteContext ctx(ns, lockReason);
                    runNonSystemInsertFromOplogWithLock(ns, row);
                }
            }
        }

        static void runCappedInsertFromOplogWithLock(
            const char* ns,
            const BSONObj& pk,
            const BSONObj& row
            )
        {
            Collection *cl = getCollection(ns);

            verify(cl->isCapped());
            CappedCollection *cappedCl = cl->as<CappedCollection>();
            // overwrite set to true because we are running on a secondary
            bool indexBitChanged = false;
            const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
            cappedCl->insertObjectWithPK(pk, row, flags, &indexBitChanged);
            // Hack copied from Collection::insertObject. TODO: find a better way to do this                        
            if (indexBitChanged) {
                cl->noteMultiKeyChanged();
            }
            cl->notifyOfWriteOp();
        }

        static void runCappedInsertFromOplog(const char *ns, const BSONObj &op) {
            const BSONObj pk = op[KEY_STR_PK].Obj();
            const BSONObj row = op[KEY_STR_ROW].Obj();
            try {
                LOCK_REASON(lockReason, "repl: applying capped insert");
                Client::ReadContext ctx(ns, lockReason);
                runCappedInsertFromOplogWithLock(ns, pk, row);
            }
            catch (RetryWithWriteLock &e) {
                LOCK_REASON(lockReason, "repl: applying capped insert with write lock");
                Client::WriteContext ctx(ns, lockReason);
                runCappedInsertFromOplogWithLock(ns, pk, row);
            }
        }

        static void runRowDelete(const BSONObj& row, Collection* cl) {
            const BSONObj pk = cl->getValidatedPKFromObject(row);
            const uint64_t flags = Collection::NO_LOCKTREE;
            deleteOneObject(cl, pk, row, flags);
        }

        static void runDeleteFromOplogWithLock(const char *ns, const BSONObj &op) {
            Collection *cl = getCollection(ns);

            const BSONObj row = op[KEY_STR_ROW].Obj();
            // Use "validated" version for paranoia, which checks for bad types like regex
            runRowDelete(row, cl);
        }

        static void runDeleteFromOplog(const char *ns, const BSONObj &op) {
            LOCK_REASON(lockReason, "repl: applying delete");
            Client::ReadContext ctx(ns, lockReason);
            runDeleteFromOplogWithLock(ns, op);
        }
        
        static void runCappedDeleteFromOplog(const char *ns, const BSONObj &op) {
            LOCK_REASON(lockReason, "repl: applying capped delete");
            Client::ReadContext ctx(ns, lockReason);
            Collection *cl = getCollection(ns);

            const BSONObj row = op[KEY_STR_ROW].Obj();
            const BSONObj pk = op[KEY_STR_PK].Obj();

            verify(cl->isCapped());
            CappedCollection *cappedCl = cl->as<CappedCollection>();
            const uint64_t flags = Collection::NO_LOCKTREE;
            cappedCl->deleteObjectWithPK(pk, row, flags);
            cl->notifyOfWriteOp();
        }

        static void runUpdateFromOplogWithLock(
            const char* ns,
            const BSONObj &pk,
            const BSONObj &oldObj,
            BSONObj newObj,
            bool isRollback
            )
        {
            Collection *cl = getCollection(ns);

            uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
            if (isRollback) {
                if (cl->isPKHidden()) {
                    // if this is a rollback, then the newObj is what is in the
                    // collections, that we want to replace with oldObj
                    // with a hidden PK, we know the PK cannot change
                    BSONObj oldObjCopy = oldObj.copy(); // to get around constness, it's rollback, so we don't care about memcpy
                    updateOneObject(cl, pk, newObj, oldObjCopy, BSONObj(), false, flags);
                }
                else {
                    // the pk is not hidden, so it may change
                    // even though this is inefficient, just do a delete followed by an insert
                    LOG(6) << "update rollback: running delete and insert on " << oldObj << " " << newObj << rsLog;
                    runRowDelete(newObj, cl);
                    runNonSystemInsertFromOplogWithLock(ns, oldObj);
                }
            }
            else {
                // normal replication case
                updateOneObject(cl, pk, oldObj, newObj, BSONObj(), false, flags);
            }
        }

        static void runUpdateFromOplog(const char *ns, const BSONObj &op, bool isRollback) {
            const char *names[] = {
                KEY_STR_PK,
                KEY_STR_OLD_ROW, 
                KEY_STR_NEW_ROW
                };
            BSONElement fields[3];
            op.getFields(3, names, fields);
            const BSONObj pk = fields[0].Obj();     // must exist
            const BSONObj oldObj = fields[1].Obj(); // must exist
            const BSONObj newObj = fields[2].Obj(); // must exist
            // must be given at least one of the new object or an update obj
            verify(!newObj.isEmpty());

            try {
                LOCK_REASON(lockReason, "repl: applying update");
                Client::ReadContext ctx(ns, lockReason);
                runUpdateFromOplogWithLock(ns, pk, oldObj, newObj, isRollback);
            }
            catch (RetryWithWriteLock &e) {
                LOCK_REASON(lockReason, "repl: applying update with write lock");
                Client::WriteContext ctx(ns, lockReason);
                runUpdateFromOplogWithLock(ns, pk, oldObj, newObj, isRollback);
            }
        }

        static void runUpdateModsWithRowWithLock(
            const char* ns,
            const BSONObj &pk,
            const BSONObj &oldObj,
            const BSONObj &updateobj,
            bool isRollback
            )
        {
            Collection *cl = getCollection(ns);
            scoped_ptr<ModSet> mods(new ModSet(updateobj, cl->indexKeys()));
            auto_ptr<ModSetState> mss = mods->prepare(oldObj);
            BSONObj newObj = mss->createNewFromMods();
            // Make sure we pass the best hint in the case of an unindexed update

            if (isRollback) {
                // we've generated the newObj and the oldObj, reuse code.
                // This code is correct for both cases, but we only run it in this
                // case because it is more inefficient.  It never uses
                // the updateUsingMods path
                runUpdateFromOplogWithLock(
                    ns,
                    pk,
                    oldObj,
                    newObj,
                    isRollback
                    );
            }
            else {
                uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
                if (mods->isIndexed() <= 0) {
                    flags |= Collection::KEYS_UNAFFECTED_HINT;
                }
                // normal replication case
                // optimization for later: if we know we are using
                // the updateUsingMods path in updateOneObject,
                // then we have constructed newObj unnecessarily
                updateOneObject(cl, pk, oldObj, newObj, updateobj, false, flags);
            }
        }

        static void runUpdateModsWithRowFromOplog(const char *ns, const BSONObj &op, bool isRollback) {
            const char *names[] = {
                KEY_STR_PK,
                KEY_STR_OLD_ROW,
                KEY_STR_MODS
                };
            BSONElement fields[3];
            op.getFields(3, names, fields);
            const BSONObj pk = fields[0].Obj();     // must exist
            const BSONObj oldObj = fields[1].Obj(); // must exist
            const BSONObj updateobj = fields[2].Obj(); // must exist
            // must be given at least one of the new object or an update obj
            verify(!updateobj.isEmpty());

            // We have an update obj and we need to create the new obj
            try {
                LOCK_REASON(lockReason, "repl: applying update");
                Client::ReadContext ctx(ns, lockReason);
                runUpdateModsWithRowWithLock(ns, pk, oldObj, updateobj, isRollback);
            }
            catch (RetryWithWriteLock &e) {
                LOCK_REASON(lockReason, "repl: applying update with write lock");
                Client::WriteContext ctx(ns, lockReason);
                runUpdateModsWithRowWithLock(ns, pk, oldObj, updateobj, isRollback);
            }
        }

        static void runCommandFromOplog(const char *ns, const BSONObj &op) {
            BufBuilder bb;
            BSONObjBuilder ob;

            // Locking and context are handled in _runCommands
            const BSONObj command = op[KEY_STR_ROW].embeddedObject();
            bool ret = _runCommands(ns, command, bb, ob, true, 0);
            massert(17220, str::stream() << "Command " << op.str() << " failed under runCommandFromOplog: " << ob.done(), ret);
        }

        static void rollbackCommandFromOplog(const char *ns, const BSONObj &op) {
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
            const char *ns = fields[0].valuestrsafe();
            const char *opType = fields[1].valuestrsafe();
            if (strcmp(opType, OP_STR_INSERT) == 0) {
                opCounters->gotInsert();
                runInsertFromOplog(ns, op);
            }
            else if (strcmp(opType, OP_STR_UPDATE) == 0) {
                opCounters->gotUpdate();
                runUpdateFromOplog(ns, op, false);
            }
            else if (strcmp(opType, OP_STR_UPDATE_ROW_WITH_MOD) == 0) {
                opCounters->gotUpdate();
                runUpdateModsWithRowFromOplog(ns, op, false);
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

        static void runRollbackInsertFromOplog(const char *ns, const BSONObj &op) {
            // handle add index case
            if (nsToCollectionSubstring(ns) == "system.indexes") {
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
            const char *ns = fields[0].valuestrsafe();
            const char *opType = fields[1].valuestrsafe();
            if (strcmp(opType, OP_STR_INSERT) == 0) {
                runRollbackInsertFromOplog(ns, op);
            }
            else if (strcmp(opType, OP_STR_UPDATE) == 0) {
                runUpdateFromOplog(ns, op, true);
            }
            else if (strcmp(opType, OP_STR_UPDATE_ROW_WITH_MOD) == 0) {
                runUpdateModsWithRowFromOplog(ns, op, true);
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

    } // namespace OplogHelpers

} // namespace mongo
