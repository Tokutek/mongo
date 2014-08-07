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
#include "mongo/db/ops/count.h"
#include "mongo/db/query_optimizer.h"

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
static const char OP_STR_UPDATE_PK_WITH_MOD[] = "up"; // update with just pk and mods to generate post-image
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
                mongoutils::str::equals(opstr, OP_STR_UPDATE_ROW_WITH_MOD) ||
                mongoutils::str::equals(opstr, OP_STR_UPDATE_PK_WITH_MOD);
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

        static void logUpdateMods(
            const char *ns,
            const BSONObj &pk,
            const BSONObj &oldObj,
            const BSONObj &updateobj,
            bool fromMigrate,
            bool onlyPK
            ) 
        {
            bool logForSharding = !fromMigrate &&
                shouldLogTxnUpdateOpForSharding(OP_STR_UPDATE, ns, oldObj);
            if (logTxnOpsForReplication() || logForSharding) {
                BSONObjBuilder b;
                if (isLocalNs(ns)) {
                    return;
                }
                if (onlyPK) {
                    appendOpType(OP_STR_UPDATE_PK_WITH_MOD, &b);
                }
                else {
                    appendOpType(OP_STR_UPDATE_ROW_WITH_MOD, &b);
                }
                appendNsStr(ns, &b);
                appendMigrate(fromMigrate, &b);
                b.append(KEY_STR_PK, pk);
                if (!onlyPK) {
                    b.append(KEY_STR_OLD_ROW, oldObj);
                }
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

        void logUpdateModsWithRow(
            const char *ns,
            const BSONObj &pk,
            const BSONObj &oldObj,
            const BSONObj &updateobj,
            bool fromMigrate
            ) 
        {
            logUpdateMods(ns, pk, oldObj, updateobj, fromMigrate, false);
        }

        void logUpdatePKModsWithRow(
            const char *ns,
            const BSONObj &pk,
            const BSONObj &oldObj,
            const BSONObj &updateobj,
            bool fromMigrate
            ) 
        {
            logUpdateMods(ns, pk, oldObj, updateobj, fromMigrate, true);
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

        // used for rolling back inserts and deletes
        static bool rowInDocsMap(const char *ns, const BSONObj &op, const char* opStr, RollbackDocsMap* docsMap) {
            if (docsMap == NULL) {
                return false;
            }
            LOCK_REASON(lockReason, "repl: checking docsMap before an insert or delete");
            Client::ReadContext ctx(ns, lockReason);
            Collection *cl = getCollection(ns);
            const BSONObj row = op[opStr].Obj();
            const BSONObj pk = cl->getValidatedPKFromObject(row);
            return docsMap->docExists(ns,pk);
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

        static void runCappedInsertFromOplog(const char *ns, const BSONObj &op, RollbackDocsMap* docsMap) {
            const BSONObj pk = op[KEY_STR_PK].Obj();
            const BSONObj row = op[KEY_STR_ROW].Obj();
            if (docsMap && docsMap->docExists(ns,pk)) {
                return; // do nothing if it is in the docsMap
            }
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
        
        static void runCappedDeleteFromOplog(const char *ns, const BSONObj &op, RollbackDocsMap* docsMap) {
            LOCK_REASON(lockReason, "repl: applying capped delete");
            const BSONObj row = op[KEY_STR_ROW].Obj();
            const BSONObj pk = op[KEY_STR_PK].Obj();
            if (docsMap && docsMap->docExists(ns,pk)) {
                return; // do nothing if it is in the docsMap
            }
            Client::ReadContext ctx(ns, lockReason);
            Collection *cl = getCollection(ns);
            verify(cl->isCapped());
            CappedCollection *cappedCl = cl->as<CappedCollection>();
            const uint64_t flags = Collection::NO_LOCKTREE;
            cappedCl->deleteObjectWithPK(pk, row, flags);
            cl->notifyOfWriteOp();
        }

        static bool runUpdateFromOplogWithDocsMap(
            const char* ns,
            const BSONObj &pk,
            const BSONObj &oldObj,
            BSONObj newObj,
            Collection *cl,
            const RollbackDocsMap* docsMap
            )
        {
            bool oldObjExistsInDocsMap = docsMap && docsMap->docExists(ns, pk);
            bool newObjExistsInDocsMap = false;
            if (cl->isPKHidden()) {
                newObjExistsInDocsMap = oldObjExistsInDocsMap;
            }
            else {
                const BSONObj newPK = cl->getValidatedPKFromObject(newObj);
                newObjExistsInDocsMap = docsMap && docsMap->docExists(ns, newPK);
            }
            if (!oldObjExistsInDocsMap && !newObjExistsInDocsMap) {
                return false;
            }
            // now we are in the weird case of one existing in docsMap
            // and the other not existing in docsMap. Not sure this is possible
            else if (!oldObjExistsInDocsMap) {
                // delete old row
                verify(!cl->isPKHidden());
                runRowDelete(oldObj, cl);
            }
            else if (!newObjExistsInDocsMap) {
                verify(!cl->isPKHidden());
                const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
                BSONObj obj = newObj;
                insertOneObject(cl, obj, flags);
            }
            else {
                // NO-OP
                // just keeping this here so I can logically reason
                // about this code in the future.
                // This is the case where both old and new exist in docsMap,
                // and in this case, we do nothing.
            }
            return true;
        }

        static void runUpdateFromOplogWithLock(
            const char* ns,
            const BSONObj &pk,
            const BSONObj &oldObj,
            BSONObj newObj,
            RollbackDocsMap* docsMap
            )
        {
            Collection *cl = getCollection(ns);

            if (runUpdateFromOplogWithDocsMap(ns, pk, oldObj, newObj, cl, docsMap)) {
                return;
            }

            uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
            updateOneObject(cl, pk, oldObj, newObj, false, flags);
        }

        static void runUpdateFromOplog(const char *ns, const BSONObj &op, RollbackDocsMap* docsMap) {
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
                runUpdateFromOplogWithLock(ns, pk, oldObj, newObj, docsMap);
            }
            catch (RetryWithWriteLock &e) {
                LOCK_REASON(lockReason, "repl: applying update with write lock");
                Client::WriteContext ctx(ns, lockReason);
                runUpdateFromOplogWithLock(ns, pk, oldObj, newObj, docsMap);
            }
        }

        static void runUpdateModsWithRowWithLock(
            const char* ns,
            const BSONObj &pk,
            const BSONObj &oldObj,
            const BSONObj &updateobj,
            const RollbackDocsMap* docsMap
            )
        {
            Collection *cl = getCollection(ns);
            scoped_ptr<ModSet> mods(new ModSet(updateobj, cl->indexKeys()));
            auto_ptr<ModSetState> mss = mods->prepare(oldObj);
            BSONObj newObj = mss->createNewFromMods();

            if (runUpdateFromOplogWithDocsMap(ns, pk, oldObj, newObj, cl, docsMap)) {
                return;
            }
            // Make sure we pass the best hint in the case of an unindexed update

            uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
            // attempt to update the object using mods. If not possible, then
            // do the more heavyweight method of using a full pre-image followed
            // by a full post image
            if (!updateOneObjectWithMods(cl, pk, updateobj, false, 0, mods.get())) {
                if (mods->isIndexed() <= 0) {
                    flags |= Collection::KEYS_UNAFFECTED_HINT;
                }
                updateOneObject(cl, pk, oldObj, newObj, false, 0);
            }
        }

        static void runUpdateModsWithRowFromOplog(const char *ns, const BSONObj &op, const RollbackDocsMap* docsMap) {
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
                runUpdateModsWithRowWithLock(ns, pk, oldObj, updateobj, docsMap);
            }
            catch (RetryWithWriteLock &e) {
                LOCK_REASON(lockReason, "repl: applying update with write lock");
                Client::WriteContext ctx(ns, lockReason);
                runUpdateModsWithRowWithLock(ns, pk, oldObj, updateobj, docsMap);
            }
        }

        static void runUpdateModsWithPKWithLock(
            const char* ns,
            const BSONObj &pk,
            const BSONObj &updateobj,
            const RollbackDocsMap* docsMap
            )
        {
            bool pkExistsInDocsMap = docsMap && docsMap->docExists(ns, pk);
            if (pkExistsInDocsMap) {
                return;
            }
            Collection *cl = getCollection(ns);
            uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
            scoped_ptr<ModSet> mods(new ModSet(updateobj, cl->indexKeys()));
            if (!updateOneObjectWithMods(cl, pk, updateobj, false, 0, mods.get())) {
                BSONObj oldObj;
                bool ret = cl->findByPK(pk, oldObj);
                verify(ret);
                auto_ptr<ModSetState> mss = mods->prepare(oldObj);
                BSONObj newObj = mss->createNewFromMods();
                if (mods->isIndexed() <= 0) {
                    flags |= Collection::KEYS_UNAFFECTED_HINT;
                }
                updateOneObject(cl, pk, oldObj, newObj, false, flags);
            }
        }

        static void runUpdateModsWithPKFromOplog(const char *ns, const BSONObj &op, const RollbackDocsMap* docsMap) {
            const char *names[] = {
                KEY_STR_PK,
                KEY_STR_MODS
                };
            BSONElement fields[2];
            op.getFields(2, names, fields);
            const BSONObj pk = fields[0].Obj();     // must exist
            const BSONObj updateobj = fields[1].Obj(); // must exist

            // We have an update obj and we need to create the new obj
            try {
                LOCK_REASON(lockReason, "repl: applying update");
                Client::ReadContext ctx(ns, lockReason);
                runUpdateModsWithPKWithLock(ns, pk, updateobj, docsMap);
            }
            catch (RetryWithWriteLock &e) {
                LOCK_REASON(lockReason, "repl: applying update with write lock");
                Client::WriteContext ctx(ns, lockReason);
                runUpdateModsWithPKWithLock(ns, pk, updateobj, docsMap);
            }
        }

        static void runCommandFromOplog(const char *ns, const BSONObj &op, RollbackDocsMap* docsMap) {
            BufBuilder bb;
            BSONObjBuilder ob;

            // Locking and context are handled in _runCommands
            const BSONObj command = op[KEY_STR_ROW].embeddedObject();
            bool ret = _runCommands(ns, command, bb, ob, true, 0);
            massert(17220, str::stream() << "Command " << op.str() << " failed under runCommandFromOplog: " << ob.done(), ret);

            // for now, every command should thrown an exception if run during
            // rollback. Soon, we will make this finer grained. For example, dropping
            // an index should not be an issue, whereas dropping or creating a
            // collection should remove all instances in docsMap for the associated
            // collection.
            if (docsMap != NULL) {
                Command* c = getCommand(command);
                massert(17359, "Could not get command", c);
                std::string dbname = nsToDatabase(ns);
                c->handleRollbackForward(dbname, command, docsMap);
            }
        }

        // apply an operation that comes from the oplog
        // If docsMap is non-NULL, that means we are running during rollback
        // and that there may be documents to ignore. It is the responsibility of
        // each case below to handle the fact that we are running during rollback
        // and act accordingly. For instance, some inserts may ignore the operation
        // because the document is in the docsMap, while a command may throw
        // a RollbackOplogException because it cannot be run during rollback
        void applyOperationFromOplog(const BSONObj& op, RollbackDocsMap* docsMap) {
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
                if (!rowInDocsMap(ns, op, KEY_STR_ROW, docsMap)) {
                    opCounters->gotInsert();
                    runInsertFromOplog(ns, op);
                }
            }
            else if (strcmp(opType, OP_STR_UPDATE) == 0) {
                opCounters->gotUpdate();
                runUpdateFromOplog(ns, op, docsMap);
            }
            else if (strcmp(opType, OP_STR_UPDATE_ROW_WITH_MOD) == 0) {
                opCounters->gotUpdate();
                runUpdateModsWithRowFromOplog(ns, op, docsMap);
            }
            else if (strcmp(opType, OP_STR_UPDATE_PK_WITH_MOD) == 0) {
                opCounters->gotUpdate();
                runUpdateModsWithPKFromOplog(ns, op, docsMap);
            }
            else if (strcmp(opType, OP_STR_DELETE) == 0) {
                if (!rowInDocsMap(ns, op, KEY_STR_ROW, docsMap)) {
                    opCounters->gotDelete();
                    runDeleteFromOplog(ns, op);
                }
            }
            else if (strcmp(opType, OP_STR_CAPPED_INSERT) == 0) {
                opCounters->gotInsert();
                runCappedInsertFromOplog(ns, op, docsMap);
            }
            else if (strcmp(opType, OP_STR_CAPPED_DELETE) == 0) {
                opCounters->gotDelete();
                runCappedDeleteFromOplog(ns, op, docsMap);
            }
            else if (strcmp(opType, OP_STR_COMMAND) == 0) {
                opCounters->gotCommand();
                runCommandFromOplog(ns, op, docsMap);
            }
            else if (strcmp(opType, OP_STR_COMMENT) == 0) {
                // no-op
            }
            else {
                throw MsgAssertionException( 14825 , ErrorMsg("error in applyOperation : unknown opType ", *opType) );
            }
        }

        static void rollbackInsertFromOplog(const char *ns, const BSONObj &op, RollbackDocsMap* docsMap) {
            // handle add index case
            if (nsToCollectionSubstring(ns) == "system.indexes") {
                throw RollbackOplogException(str::stream() << "Not rolling back an add index on " << ns << ". Op: " << op.toString(false, true));
            }
            else {
                // the rollback of a normal insert is to do the delete
                if (!rowInDocsMap(ns, op, KEY_STR_ROW, docsMap)) {
                    runDeleteFromOplog(ns, op);
                }
            }
        }

        static void rollbackUpdateFromOplog(const char *ns, const BSONObj &op, RollbackDocsMap* docsMap) {
            const BSONObj pk = op[KEY_STR_PK].Obj();
            const BSONObj newObj = op[KEY_STR_NEW_ROW].Obj(); // must exist
            LOCK_REASON(lockReason, "repl: checking newRow for rollback");
            Client::ReadContext ctx(ns, lockReason);
            Collection *cl = getCollection(ns);
            if (!cl->isPKHidden()) {
                docsMap->addDoc(ns, pk);
                const BSONObj newPK = cl->getValidatedPKFromObject(newObj);
                docsMap->addDoc(ns, newPK);
            }
            else {
                // do the reverse update
                BSONObj oldObj = op[KEY_STR_OLD_ROW].Obj(); // must exist
                runUpdateFromOplogWithLock(
                    ns,
                    pk,
                    newObj,
                    oldObj,
                    NULL
                    );                
            }
        }

        static void rollbackUpdateModsFromOplog(const char *ns, const BSONObj &op, RollbackDocsMap* docsMap) {
            const BSONObj pk = op[KEY_STR_PK].Obj();
            const BSONObj oldObj = op[KEY_STR_OLD_ROW].Obj(); // must exist
            const BSONObj updateobj = op[KEY_STR_MODS].Obj(); // must exist
            {
                LOCK_REASON(lockReason, "repl: checking newRow with mods for rollback");
                Client::ReadContext ctx(ns, lockReason);
                Collection *cl = getCollection(ns);
                scoped_ptr<ModSet> mods(new ModSet(updateobj, cl->indexKeys()));
                auto_ptr<ModSetState> mss = mods->prepare(oldObj);
                BSONObj newObj = mss->createNewFromMods();
                if (!cl->isPKHidden()) {
                    docsMap->addDoc(ns, pk);
                    const BSONObj newPK = cl->getValidatedPKFromObject(newObj);
                    docsMap->addDoc(ns, newPK);
                }
                else {
                    runUpdateFromOplogWithLock(
                        ns,
                        pk,
                        newObj,
                        oldObj,
                        NULL
                        );                
                }
            }
        }

        static void rollbackUpdatePKModsFromOplog(const char *ns, const BSONObj &op, RollbackDocsMap* docsMap) {
            const BSONObj pk = op[KEY_STR_PK].Obj();
            const BSONObj updateobj = op[KEY_STR_MODS].Obj(); // must exist
            {
                LOCK_REASON(lockReason, "repl: checking newRow with mods for rollback");
                Client::ReadContext ctx(ns, lockReason);
                Collection *cl = getCollection(ns);
                verify(!cl->isPKHidden()); // sanity check
                docsMap->addDoc(ns, pk);
            }
        }

        static void rollbackDeleteFromOplog(const char *ns, const BSONObj &op, RollbackDocsMap* docsMap) {
            // the rollback of a delete is to do the insert
            if (!rowInDocsMap(ns, op, KEY_STR_ROW, docsMap)) {
                runInsertFromOplog(ns, op);
            }
        }

        static void rollbackCappedInsertFromOplog(const char *ns, const BSONObj &op, RollbackDocsMap* docsMap) {
            runCappedDeleteFromOplog(ns, op, docsMap);
        }

        static void rollbackCappedDeleteFromOplog(const char *ns, const BSONObj &op, RollbackDocsMap* docsMap) {
            runCappedInsertFromOplog(ns, op, docsMap);
        }

        static void rollbackCommandFromOplog(const char *ns, const BSONObj &op) {
            BSONObj command = op[KEY_STR_ROW].embeddedObject();
            log() << "Cannot rollback command " << op << rsLog;
            throw RollbackOplogException(str::stream() << "Could not rollback command " << command << " on ns " << ns);
        }

        void rollbackOperationFromOplog(const BSONObj& op, RollbackDocsMap* docsMap) {
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
                rollbackInsertFromOplog(ns, op, docsMap);
            }
            else if (strcmp(opType, OP_STR_UPDATE) == 0) {
                rollbackUpdateFromOplog(ns, op, docsMap);
            }
            else if (strcmp(opType, OP_STR_UPDATE_ROW_WITH_MOD) == 0) {
                rollbackUpdateModsFromOplog(ns, op, docsMap);
            }
            else if (strcmp(opType, OP_STR_UPDATE_PK_WITH_MOD) == 0) {
                rollbackUpdatePKModsFromOplog(ns, op, docsMap);
            }
            else if (strcmp(opType, OP_STR_DELETE) == 0) {
                rollbackDeleteFromOplog(ns, op, docsMap);
            }
            else if (strcmp(opType, OP_STR_CAPPED_INSERT) == 0) {
                rollbackCappedInsertFromOplog(ns, op, docsMap);
            }
            else if (strcmp(opType, OP_STR_CAPPED_DELETE) == 0) {
                rollbackCappedDeleteFromOplog(ns, op, docsMap);
            }
            else if (strcmp(opType, OP_STR_COMMAND) == 0) {
                rollbackCommandFromOplog(ns, op);
            }
            else if (strcmp(opType, OP_STR_COMMENT) == 0) {
                // no-op
            }
            else {
                throw MsgAssertionException( 16795 , ErrorMsg("error in applyOperation : unknown opType ", *opType) );
            }
        }

    } // namespace OplogHelpers

    void RollbackDocsMap::initialize() {
        LOCK_REASON(lockReason, "repl rollback: initializing RollbackDocsMap");
        Client::WriteContext ctx(rsRollbackDocs, lockReason);
        Client::Transaction transaction(DB_SERIALIZABLE);
        string errmsg;
        Collection* cl = getCollection(rsRollbackDocs);
        if (cl != NULL) {
            BSONObjBuilder result;
            cl->drop(errmsg,result);
            cl = NULL;
        }
        bool ret = userCreateNS(rsRollbackDocs, BSONObj(), errmsg, false);
        verify(ret);
        // sanity check that we get the collection
        cl = getCollection(rsRollbackDocs);
        verify(cl);
        transaction.commit();
    }

    void RollbackDocsMap::dropDocsMap() {
        LOCK_REASON(lockReason, "repl rollback: dropping RollbackDocsMap");
        Client::WriteContext ctx(rsRollbackDocs, lockReason);
        string errmsg;
        Collection* cl = getCollection(rsRollbackDocs);
        if (cl != NULL) {
            BSONObjBuilder result;
            cl->drop(errmsg,result);
        }
    }

    bool RollbackDocsMap::docExists(const char* ns, const BSONObj pk) const {
        // this function should be called from repl with a transaction already created
        LOCK_REASON(lockReason, "repl rollback: RollbackDocsMap::docExists");
        Client::ReadContext ctx(rsRollbackDocs, lockReason);
        BSONObj result;
        Collection* cl = getCollection(rsRollbackDocs);
        verify(cl);
        BSONObjBuilder docBuilder;
        BSONObjBuilder idBuilder(docBuilder.subobjStart(""));
        idBuilder.append("ns", ns);
        idBuilder.append("pk", pk);
        idBuilder.done();
        bool ret = cl->findByPK(docBuilder.done(), result);
        return ret;

    }

    bool RollbackDocsMap::docsForNSExists(const char* ns) const {
        LOCK_REASON(lockReason, "repl rollback: RollbackDocsMap::docsForNSExists");
        Client::ReadContext ctx(rsRollbackDocs, lockReason);
        BSONObjBuilder nsBuilder;
        nsBuilder.append("ns" , ns);
        BSONObjBuilder gteBuilder;
        gteBuilder.append("$gte", nsBuilder.obj());
        BSONObjBuilder query;
        query.append("_id", gteBuilder.obj());
        // now we have our query: {_id : {$gte : { ns : ns  }}}
        BSONObj result;
        bool found = Collection::findOne(rsRollbackDocs, query.done(), result, true);
        if (!found) {
            return false;
        }
        BSONObj id = result["_id"].Obj();
        return strcmp(ns, id["ns"].String().c_str()) == 0;
    }

    void RollbackDocsMap::addDoc(const char* ns, const BSONObj pk) {
        // this function should be called from repl with a transaction already created
        LOCK_REASON(lockReason, "repl rollback: RollbackDocsMap::addDoc");
        Client::ReadContext ctx(rsRollbackDocs, lockReason);
        Collection* cl = getCollection(rsRollbackDocs);
        verify(cl);
        BSONObjBuilder docBuilder;
        BSONObjBuilder idBuilder(docBuilder.subobjStart("_id"));
        idBuilder.append("ns", ns);
        idBuilder.append("pk", pk);
        idBuilder.done();
        // by having NO_UNIQUE_CHECKS, I don't need to worry about inserting the
        // same thing multiple times, it will just overwrite the doc with an identical one
        const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
        BSONObj doc = docBuilder.obj();
        insertOneObject(cl, doc, flags);                                
    }

    long long RollbackDocsMap::size() {
        LOCK_REASON(lockReason, "repl rollback: getting size of RollbackDocsMap");
        Client::ReadContext ctx(rsRollbackDocs, lockReason);
        Client::Transaction transaction(DB_READ_UNCOMMITTED);
        string err;
        int errCode;
        long long ret = runCount(rsRollbackDocs, BSONObj(), err, errCode);
        verify(ret >= 0);
        transaction.commit();
        return ret;
    }

    void RollbackDocsMap::startIterator() {
        LOCK_REASON(lockReason, "repl rollback: starting iterator of rollback docs");
        Client::ReadContext ctx(rsRollbackDocs, lockReason);
        BSONObjBuilder queryBuilder;
        BSONObjBuilder idBuilder(queryBuilder.subobjStart("_id"));
        idBuilder.appendMinKey("$gt");
        idBuilder.done();
        BSONObj result;
        LOG(2) << "starting iterator with query " << queryBuilder.done() << rsLog;
        bool ret = Collection::findOne(rsRollbackDocs, queryBuilder.done(), result, false);
        LOG(2) << "iterator started with " << ret << " returned" << rsLog;
        if (ret) {
            _current = result["_id"].Obj().copy();
            LOG(2) << "Setting _current of iterator to " << _current << rsLog;
        }
        else {
            _current = BSONObj();
        }
    }
    bool RollbackDocsMap::ok() {
        return !_current.isEmpty();
    }
    void RollbackDocsMap::advance() {
        verify(ok());
        LOCK_REASON(lockReason, "repl rollback: advancing iterator of rollback docs");
        Client::ReadContext ctx(rsRollbackDocs, lockReason);
        const BSONObj query = BSON("_id" << BSON("$gt" << _current));
        BSONObj result;
        LOG(2) << "advancing iterator with query " << query << rsLog;
        bool ret = Collection::findOne(rsRollbackDocs, query, result, true);
        LOG(2) << "iterator advanced with " << ret << " returned" << rsLog;
        if (ret) {
            _current = result["_id"].Obj().copy();
            LOG(2) << "Setting _current of iterator to " << _current << rsLog;
        }
        else {
            _current = BSONObj();
        }
    }

    DocID RollbackDocsMap::current() {
        verify(ok());
        return DocID(_current["ns"].String().c_str(), _current["pk"].Obj());
    }
} // namespace mongo
