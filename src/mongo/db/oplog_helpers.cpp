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
#include "mongo/db/namespace_details.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/stats/counters.h"

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

namespace mongo {

    namespace OpLogHelpers {

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
        
        void logInsert(const char *ns, const BSONObj &row) {
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

        void logUpdate(const char *ns, const BSONObj& pk,
                       const BSONObj& oldRow, const BSONObj& newRow,
                       bool fromMigrate) {
            bool logForSharding = !fromMigrate &&
                shouldLogTxnUpdateOpForSharding(OP_STR_UPDATE, ns, oldRow, newRow);
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
                    cc().txn().logOpForReplication(logObj);
                }
                if (logForSharding) {
                    cc().txn().logOpForSharding(logObj);
                }
            }
        }

        void logUpdateMods(const char *ns, const BSONObj &pk,
                           const BSONObj &updateobj,
                           bool fromMigrate) {
            bool logForSharding = !fromMigrate &&
                shouldLogTxnUpdateOpForSharding(OP_STR_UPDATE, ns, pk, pk); // HACK: Faking newObj and oldObj with just PK
            if (logTxnOpsForReplication() || logForSharding) {
                BSONObjBuilder b;
                if (isLocalNs(ns)) {
                    return;
                }

                appendOpType(OP_STR_UPDATE_MODS, &b);
                appendNsStr(ns, &b);
                appendMigrate(fromMigrate, &b);
                b.append(KEY_STR_PK, pk);
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

        static void runColdIndexFromOplog(const char *ns, const BSONObj &row) {
            Client::WriteContext ctx(ns);
            NamespaceDetails* nsd = nsdetails(ns);
            const string &coll = row["ns"].String();

            NamespaceDetails* collNsd = nsdetails(coll);
            const bool ok = collNsd->ensureIndex(row);
            if (!ok) {
                // the index already exists, so this is a no-op
                // Note that for create index and drop index, we
                // are tolerant of the fact that the operation may
                // have already been done
                return;
            }
            BSONObj obj = row;
            insertOneObject(nsd, obj, NamespaceDetails::NO_UNIQUE_CHECKS);
        }

        static void runHotIndexFromOplog(const char *ns, const BSONObj &row) {
            // The context and lock must outlive the indexer so that
            // the indexer destructor gets called in a write locked.
            // These MUST NOT be reordered, the context must destruct
            // after the indexer.
            scoped_ptr<Lock::DBWrite> lk(new Lock::DBWrite(ns));
            scoped_ptr<NamespaceDetails::HotIndexer> indexer;

            {
                Client::Context ctx(ns);
                NamespaceDetails* nsd = nsdetails(ns);

                const string &coll = row["ns"].String();
                NamespaceDetails* collNsd = nsdetails(coll);
                if (collNsd->findIndexByKeyPattern(row["key"].Obj()) >= 0) {
                    // the index already exists, so this is a no-op
                    // Note that for create index and drop index, we
                    // are tolerant of the fact that the operation may
                    // have already been done
                    return;
                }
                BSONObj obj = row;
                insertOneObject(nsd, obj, NamespaceDetails::NO_UNIQUE_CHECKS);
                indexer.reset(new NamespaceDetails::HotIndexer(collNsd, row));
                indexer->prepare();
            }

            {
                Lock::DBWrite::Downgrade dg(lk);
                Client::Context ctx(ns);
                indexer->build();
            }

            {
                Client::Context ctx(ns);
                indexer->commit();
            }
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
                Client::ReadContext ctx(ns);
                NamespaceDetails* nsd = nsdetails(ns);

                // overwrite set to true because we are running on a secondary
                uint64_t flags = (NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE);        
                BSONObj obj = row;
                insertOneObject(nsd, obj, flags);
            }
        }

        static void runCappedInsertFromOplog(const char *ns, const BSONObj &op) {
            const BSONObj pk = op[KEY_STR_PK].Obj();
            const BSONObj row = op[KEY_STR_ROW].Obj();

            Client::ReadContext ctx(ns);
            NamespaceDetails *nsd = nsdetails(ns);

            verify(nsd->isCapped());
            CappedCollection *cl = nsd->toSubclass<CappedCollection>();
            // overwrite set to true because we are running on a secondary
            const uint64_t flags = NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE;
            cl->insertObjectWithPK(pk, row, flags);
            cl->notifyOfWriteOp();
        }

        static void runDeleteFromOplog(const char *ns, const BSONObj &op) {
            Client::ReadContext ctx(ns);
            NamespaceDetails* nsd = nsdetails(ns);

            const BSONObj row = op[KEY_STR_ROW].Obj();
            // Use "validated" version for paranoia, which checks for bad types like regex
            const BSONObj pk = nsd->getValidatedPKFromObject(row);
            const uint64_t flags = NamespaceDetails::NO_LOCKTREE;
            deleteOneObject(nsd, pk, row, flags);
        }
        
        static void runCappedDeleteFromOplog(const char *ns, const BSONObj &op) {
            Client::ReadContext ctx(ns);
            NamespaceDetails* nsd = nsdetails(ns);

            const BSONObj row = op[KEY_STR_ROW].Obj();
            const BSONObj pk = op[KEY_STR_PK].Obj();

            verify(nsd->isCapped());
            CappedCollection *cl = nsd->toSubclass<CappedCollection>();
            const uint64_t flags = NamespaceDetails::NO_LOCKTREE;
            cl->deleteObjectWithPK(pk, row, flags);
            cl->notifyOfWriteOp();
        }

        static void runUpdateFromOplog(const char *ns, const BSONObj &op, bool isRollback) {
            Client::ReadContext ctx(ns);
            NamespaceDetails* nsd = nsdetails(ns);

            const char *names[] = {
                KEY_STR_PK,
                KEY_STR_OLD_ROW, 
                KEY_STR_NEW_ROW
                };
            BSONElement fields[3];
            op.getFields(3, names, fields);
            const BSONObj pk = fields[0].Obj();
            const BSONObj oldRow = fields[1].Obj();
            const BSONObj newRow = fields[2].Obj();
            // note the only difference between these two cases is
            // what is passed as the before image, and what is passed
            // as after. In normal replication, we replace oldRow with newRow.
            // In rollback, we replace newRow with oldRow
            uint64_t flags = (NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE);        
            if (isRollback) {
                // if this is a rollback, then the newRow is what is in the
                // collections, that we want to replace with oldRow
                updateOneObject(nsd, pk, newRow, oldRow, false, false, flags);
            }
            else {
                // normal replication case
                updateOneObject(nsd, pk, oldRow, newRow, false, false, flags);
            }
        }

        static void runUpdateModsFromOplog(const char *ns, const BSONObj &op, bool isRollback) {
            Client::ReadContext ctx(ns);
            NamespaceDetails* nsd = nsdetails(ns);

            const char *names[] = { KEY_STR_PK, KEY_STR_MODS };
            BSONElement fields[2];
            op.getFields(2, names, fields);
            const BSONObj pk = fields[0].Obj();
            const BSONObj updateobj = fields[1].Obj();
            uint64_t flags = (NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE);        
            updateByPK(nsd, pk, pk /* patternOrig, the "full" query */,
                       // for rollback, we need to invert the update object to 'roll back' the update.
                       !isRollback ? updateobj : invertUpdateMods(updateobj),
                       false, true /* fastupdates always okay on secondary */,
                       false, false, flags);
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
            else if (strcmp(opType, OP_STR_UPDATE_MODS) == 0) {
                opCounters->gotUpdate();
                runUpdateModsFromOplog(ns, op, false);
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
            else if (strcmp(opType, OP_STR_UPDATE_MODS) == 0) {
                runUpdateModsFromOplog(ns, op, true);
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
