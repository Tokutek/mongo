// @file oplog.cpp

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

#include <vector>

#include "mongo/base/counter.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/crash.h"
#include "mongo/db/oplog.h"
#include "mongo/db/cmdline.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/repl_block.h"
#include "mongo/db/repl.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/stats/timer_stats.h"
#include "mongo/db/query_optimizer_internal.h"
#include "mongo/db/collection.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/instance.h"
#include "mongo/db/repl/bgsync.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/jsobjmanipulator.h"
#include "mongo/util/elapsed_tracker.h"
#include "mongo/db/storage/assert_ids.h"
#include "mongo/db/server_parameters.h"

namespace mongo {

    void deleteOplogFiles() {
        Client::Context ctx(rsoplog, dbpath);
        // TODO: code review this for possible error cases
        // although, I don't think we care about error cases,
        // just that after we exit, oplog files don't exist
        Collection *cl;
        BSONObjBuilder out;
        string errmsg;

        cl = getCollection(rsoplog);
        if (cl != NULL) {
            cl->drop(errmsg, out);
        }
        cl = getCollection(rsOplogRefs);
        if (cl != NULL) {
            cl->drop(errmsg, out);
        }
        cl = getCollection(rsReplInfo);
        if (cl != NULL) {
            cl->drop(errmsg, out);
        }
    }

    // assumes it is locked on entry
    void logToReplInfo(GTID minLiveGTID, GTID minUnappliedGTID) {
        Client::Context ctx(rsOplogRefs , dbpath);
        Collection* replInfoDetails = getCollection(rsReplInfo);
        BufBuilder bufbuilder(256);
        BSONObjBuilder b(bufbuilder);
        b.append("_id", "minLive");
        addGTIDToBSON("GTID", minLiveGTID, b);
        BSONObj bb = b.done();
        const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
        replInfoDetails->insertObject(bb, flags);

        bufbuilder.reset();
        BSONObjBuilder b2(bufbuilder);
        b2.append("_id", "minUnapplied");
        addGTIDToBSON("GTID", minUnappliedGTID, b2);
        BSONObj bb2 = b2.done();
        replInfoDetails->insertObject(bb2, flags);
    }

    void logHighestVotedForPrimary(uint64_t hkp) {
        Collection* replInfoDetails = getCollection(rsVoteInfo);
        verify(replInfoDetails);
        BufBuilder bufbuilder(256);
        BSONObjBuilder b(bufbuilder);
        b.append("_id", "highestVote");
        b.append("val", hkp);
        BSONObj bb = b.done();
        const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
        replInfoDetails->insertObject(bb, flags);
    }

    static void _writeEntryToOplog(BSONObj entry) {
        Collection* rsOplogDetails = getCollection(rsoplog);
        verify(rsOplogDetails);

        const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
        rsOplogDetails->insertObject(entry, flags);
    }

    static void writeEntryToOplog(BSONObj entry) {
        TimerHolder insertTimer(&oplogInsertStats);
        oplogInsertBytesStats.increment(entry.objsize());
        _writeEntryToOplog(entry);
    }
    
    void logTransactionOps(GTID gtid, uint64_t timestamp, uint64_t hash, const deque<BSONObj>& ops) {
        LOCK_REASON(lockReason, "repl: logging to oplog");
        Client::ReadContext ctx(rsoplog, lockReason);

        BSONObjBuilder b;
        addGTIDToBSON("_id", gtid, b);
        b.appendDate("ts", timestamp);
        b.append("h", (long long)hash);
        b.append("a", true);

        BSONArrayBuilder opInfoBuilder(b.subarrayStart("ops"));
        for (deque<BSONObj>::const_iterator it = ops.begin(); it != ops.end(); it++) {
            opInfoBuilder.append(*it);
        }
        opInfoBuilder.done();

        BSONObj bb = b.done();
        // write it to oplog
        LOG(3) << "writing " << bb.toString(false, true) << " to master " << endl;
        writeEntryToOplog(bb);
    }

    static void updateMaxRefGTID(BSONObj refMeta, uint64_t i, PartitionedCollection* pc, GTID gtid) {
        BSONObjBuilder bbb;
        cloneBSONWithFieldChanged(bbb, refMeta, "maxRefGTID", gtid);
        BSONElement gtidElement = refMeta["maxRefGTID"];
        if (!gtidElement.ok()) {
            log() << "maxRefGTID expected, but not found in partition " << 
                i << ". Unless this is an upgrade, this is unexpected." << endl;
            bbb.append("maxRefGTID", gtid);
        }
        pc->updatePartitionMetadata(i, bbb.done());
    }

    void logTransactionOpsRef(GTID gtid, uint64_t timestamp, uint64_t hash, OID& oid) {
        LOCK_REASON(lockReason, "repl: logging to oplog");
        Client::ReadContext ctx(rsoplog, lockReason);
        BSONObjBuilder b;
        addGTIDToBSON("_id", gtid, b);
        b.appendDate("ts", timestamp);
        b.append("h", (long long)hash);
        b.append("a", true);
        b.append("ref", oid);
        BSONObj bb = b.done();
        writeEntryToOplog(bb);
        // If the OID has elements that are not in the last partition,
        // then we need to update the last partition's metadata to reflect
        // this, so when it comes time to trimming, we don't
        // accidentally trim a piece of oplog.refs that is still referenced
        // by an existing piece of the oplog
        {
            Collection* rsOplogRefsDetails = getCollection(rsOplogRefs);
            verify(rsOplogRefsDetails);
            PartitionedCollection* pc = rsOplogRefsDetails->as<PartitionedCollection>();
            uint64_t numRef = pc->numPartitions();

            uint64_t minPartitionInserted = 0;
            {
                BSONObjBuilder b;            
                // build the _id
                BSONObjBuilder b_id( b.subobjStart( "" ) );
                b_id.append("oid", oid);
                // probably not necessary to increment _seq, but safe to do
                b_id.append("seq", 0);
                b_id.done();
                minPartitionInserted = pc->partitionWithPK(b.done());
            }
            // only update metadata if there are insertions that happened
            // in partitions OTHER than the last partition ( < numRef-1)
            if (minPartitionInserted < numRef - 1) {
                // use an alternate transaction stack,
                // so that this work does not get lumped in with
                // the rest of the transaction's work (which can be big)
                // If this transaction commits and the current "big" one
                // does not, that's ok. The maxRefGTID will be bigger
                // than it has to be, and that is benign.
                Client::AlternateTransactionStack altStack;
                Client::Transaction txn(DB_SERIALIZABLE);
                for (uint64_t i = minPartitionInserted; i < numRef - 1; i++) {
                    // for each one, update metadata
                    BSONObj refMeta = pc->getPartitionMetadata(i);
                    GTID currGTID = getGTIDFromBSON("maxRefGTID", refMeta);
                    if (GTID::cmp(currGTID, gtid) >= 0) {
                        // currGTID is already higher, do nothing and return
                        continue;
                    }
                    updateMaxRefGTID(refMeta, i, pc, gtid);
                }
                txn.commit();
            }
        }
    }

    void logOpsToOplogRef(BSONObj o) {
        LOCK_REASON(lockReason, "repl: logging to oplog.refs");
        Client::ReadContext ctx(rsOplogRefs, lockReason);
        writeEntryToOplogRefs(o);
    }

    void createOplog() {
        bool rs = !cmdLine._replSet.empty();
        verify(rs);
        
        const char * oplogNS = rsoplog;
        const char * replInfoNS = rsReplInfo;
        Client::Context ctx(oplogNS);
        Collection * oplogNSD = getCollection(oplogNS);
        Collection * oplogRefsNSD = getCollection(rsOplogRefs);        
        Collection * replInfoNSD = getCollection(replInfoNS);
        if (oplogNSD || replInfoNSD || oplogRefsNSD) {
            // TODO: (Zardosht), figure out if there are any checks to do here
            // not sure under what scenarios we can be here, so
            // making a printf to catch this so we can investigate
            tokulog() << "createOplog called with existing collections, investigate why.\n" << endl;
            return;
        }

        /* create an oplog collection, if it doesn't yet exist. */
        BSONObjBuilder b;
        b.append("partitioned", 1);
        // create the namespace
        string err;
        BSONObj o = b.done();
        bool ret;
        ret = userCreateNS(oplogNS, o, err, false);
        verify(ret);
        ret = userCreateNS(rsOplogRefs, o, err, false);
        verify(ret);
        ret = userCreateNS(replInfoNS, BSONObj(), err, false);
        verify(ret);
    }

    GTID getGTIDFromOplogEntry(BSONObj o) {
        return getGTIDFromBSON("_id", o);
    }

    bool getLastGTIDinOplog(GTID* gtid) {
        LOCK_REASON(lockReason, "repl: looking up last GTID in oplog");
        Client::ReadContext ctx(rsoplog, lockReason);
        Collection *cl = getCollection(rsoplog);
        shared_ptr<Cursor> c( Cursor::make(cl, -1) );
        if (c->ok()) {
            *gtid = getGTIDFromOplogEntry(c->current());
            return true;
        }
        return false;
    }

    bool gtidExistsInOplog(GTID gtid) {
        LOCK_REASON(lockReason, "repl: querying for GTID in oplog");
        Client::ReadContext ctx(rsoplog, lockReason);
        BSONObjBuilder q;
        BSONObj result;
        addGTIDToBSON("_id", gtid, q);
        const bool found = Collection::findOne(rsoplog, q.done(), result);
        return found;
    }

    void writeEntryToOplogRefs(BSONObj o) {
        Collection* rsOplogRefsDetails = getCollection(rsOplogRefs);
        verify(rsOplogRefsDetails);

        TimerHolder insertTimer(&oplogInsertStats);
        oplogInsertBytesStats.increment(o.objsize());

        const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
        rsOplogRefsDetails->insertObject(o, flags);
    }

    // assumes oplog is read locked on entry
    void replicateTransactionToOplog(BSONObj& op) {
        // set the applied bool to false, to let the oplog know that
        // this entry has not been applied to collections
        BSONElementManipulator(op["a"]).setBool(false);
        writeEntryToOplog(op);
    }

    MONGO_EXPORT_SERVER_PARAMETER(soTimeoutForReplLargeTxn, double, 600.0);

    void replicateFullTransactionToOplog(BSONObj& o, OplogReader& r, bool* bigTxn) {
        *bigTxn = false;
        if (o.hasElement("ref")) {
            OID oid = o["ref"].OID();
            LOG(3) << "oplog ref " << oid << endl;
            // set the socket timeout to be unlimited, because we are trying to replicate
            // a large transaction. We will reset it at the end of this if-clause
            // If something throws in here, the oplog reader ought to get destroyed,
            // so no need for an RAII style of resetting
            r.setSocketTimeout(soTimeoutForReplLargeTxn);
            shared_ptr<DBClientCursor> c = r.getOplogRefsCursor(oid);
            uassert(17363, "Could not get oplog refs cursor", c.get());
            // we are doing the work of copying oplog.refs data and writing to oplog
            // underneath a read lock
            // to ensure that neither oplog or oplog.refs has a partition
            // added, so that the trickery required in logTransactionOpsRef
            // to update maxRefGTID is not necessary.
            // If we release the read lock between copying oplog refs range
            // and writing to oplog, we would have to handle the case
            // where a partition is added while copying the range
            LOCK_REASON(lockReason, "repl: copying oplog.refs range");
            Client::ReadContext ctx(rsOplogRefs, lockReason);
            while (c->more()) {
                BSONObj b = c->next();
                BSONElement eOID = b.getFieldDotted("_id.oid");
                if (oid != eOID.OID()) {
                    break;
                }
                LOG(6) << "copyOplogRefsRange " << b << endl;
                writeEntryToOplogRefs(b);
            }
            replicateTransactionToOplog(o);
            *bigTxn = true;
            r.resetSocketTimeout();
        }
        else {
            LOCK_REASON(lockReason, "repl: copying entry to local oplog");
            Client::ReadContext ctx(rsoplog, lockReason);
            replicateTransactionToOplog(o);
        }
    }

    // apply all operations in the array
    static void applyOps(std::vector<BSONElement> ops, RollbackDocsMap* docsMap) {
        const size_t numOps = ops.size();
        for(size_t i = 0; i < numOps; ++i) {
            BSONElement* curr = &ops[i];
            OplogHelpers::applyOperationFromOplog(curr->Obj(), docsMap);
        }
    }

    // find all oplog entries for a given OID in the oplog.refs collection and apply them
    // TODO this should be a range query on oplog.refs where _id.oid == oid and applyOps to
    // each entry found.  The locking of the query interleaved with the locking in the applyOps
    // did not work, so it a sequence of point queries.  
    // TODO verify that the query plan is a indexed lookup.
    // TODO verify that the query plan does not fetch too many docs and then only process one of them.
    void applyRefOp(BSONObj entry, RollbackDocsMap* docsMap) {
        OID oid = entry["ref"].OID();
        LOG(3) << "apply ref " << entry << " oid " << oid << endl;
        long long seq = 0; // note that 0 is smaller than any of the seq numbers
        while (1) {
            BSONObj entry;
            {
                LOCK_REASON(lockReason, "repl: finding oplog.refs entry to apply");
                Client::ReadContext ctx(rsOplogRefs, lockReason);
                const BSONObj query = BSON("_id" << BSON("$gt" << BSON("oid" << oid << "seq" << seq)));
                if (!Collection::findOne(rsOplogRefs, query, entry, true)) {
                    break;
                }
            }
            BSONElement e = entry.getFieldDotted("_id.seq");
            seq = e.Long();
            BSONElement eOID = entry.getFieldDotted("_id.oid");
            if (oid != eOID.OID()) {
                break;
            }
            LOG(3) << "apply " << entry << " seq=" << seq << endl;
            applyOps(entry["ops"].Array(), docsMap);
        }
    }

    void updateApplyBitToEntry(BSONObj entry, bool apply) {
        Collection* rsOplogDetails = getCollection(rsoplog);
        verify(rsOplogDetails);
        const BSONObj pk = rsOplogDetails->getValidatedPKFromObject(entry);
        const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;

        BSONObjBuilder b;
        // build the _id
        BSONObjBuilder b_id( b.subobjStart( "$set" ) );
        b_id.append("a", apply);
        b_id.done();

        rsOplogDetails->updateObjectMods(pk, b.done(), BSONObj(), 0, false, flags);
    }
    
    // takes an entry that was written _logTransactionOps
    // and applies them to collections
    //
    // TODO: possibly improve performance of this. We create and destroy a
    // context for each operation. Find a way to amortize it out if necessary
    //
    void applyTransactionFromOplog(BSONObj entry, RollbackDocsMap* docsMap) {
        bool transactionAlreadyApplied = entry["a"].Bool();
        if (!transactionAlreadyApplied) {
            Client::Transaction transaction(DB_SERIALIZABLE);
            if (entry.hasElement("ref")) {
                applyRefOp(entry, docsMap);
            } else if (entry.hasElement("ops")) {
                applyOps(entry["ops"].Array(), docsMap);
            } else {
                verify(0);
            }
            // set the applied bool to true, to let the oplog know that
            // this entry has been applied to collections
            {
                LOCK_REASON(lockReason, "repl: setting oplog entry's applied bit");
                Client::ReadContext ctx(rsoplog, lockReason);
                updateApplyBitToEntry(entry, true);
            }
            // If this code fails, it is impossible to recover from
            // because we don't know if the transaction successfully committed
            // so we might as well crash
            // There is currently no known way this code can throw an exception
            try {
                // we are operating as a secondary. We don't have to fsync
                transaction.commit(DB_TXN_NOSYNC);
            }
            catch (std::exception &e) {
                StackStringBuilder ssb;
                ssb << "exception during commit of applyTransactionFromOplog, aborting system: " << e.what();
                dumpCrashInfo(ssb.str());
                ::abort();
            }
        }
    }
    
    // apply all operations in the array
    static void rollbackOps(std::vector<BSONElement> ops, RollbackDocsMap* docsMap, RollbackSaveData* rsSave, GTID gtid) {
        const size_t numOps = ops.size();
        for(size_t i = 0; i < numOps; ++i) {
            // note that we have to rollback the transaction backwards
            BSONElement* curr = &ops[numOps - i - 1];
            OplogHelpers::rollbackOperationFromOplog(curr->Obj(), docsMap);
            rsSave->saveOp(gtid, curr->Obj());
        }
    }

    static void rollbackRefOp(BSONObj entry, RollbackDocsMap* docsMap, RollbackSaveData* rsSave, GTID gtid) {
        OID oid = entry["ref"].OID();
        LOG(3) << "rollback ref " << entry << " oid " << oid << endl;
        long long seq = LLONG_MAX;
        while (1) {
            BSONObj currEntry;
            {
                LOCK_REASON(lockReason, "repl: rolling back entry from oplog.refs");
                Client::ReadContext ctx(rsOplogRefs, lockReason);
                Collection* rsOplogRefsDetails = getCollection(rsOplogRefs);
                verify(rsOplogRefsDetails != NULL);
                shared_ptr<Cursor> c(
                    Cursor::make(
                        rsOplogRefsDetails,
                        rsOplogRefsDetails->getPKIndex(),
                        KeyPattern::toKeyFormat(BSON( "_id" << BSON("oid" << oid << "seq" << seq))), // right endpoint
                        KeyPattern::toKeyFormat(BSON( "_id" << BSON("oid" << oid << "seq" << 0))), // left endpoint
                        false,
                        -1 // direction
                        )
                    );
                if (c->ok()) {
                    currEntry = c->current().copy();
                }
                else {
                    break;
                }
            }
            BSONElement e = currEntry.getFieldDotted("_id.seq");
            seq = e.Long();
            BSONElement eOID = currEntry.getFieldDotted("_id.oid");
            if (oid != eOID.OID()) {
                break;
            }
            LOG(3) << "apply " << currEntry << " seq=" << seq << endl;
            rollbackOps(currEntry["ops"].Array(), docsMap, rsSave, gtid);
            // decrement seq so next query gets the next value
            seq--;
        }
    }

    void rollbackTransactionFromOplog(BSONObj entry, RollbackDocsMap* docsMap, RollbackSaveData* rsSave) {
        bool transactionAlreadyApplied = entry["a"].Bool();
        GTID gtid = getGTIDFromOplogEntry(entry);
        Client::Transaction transaction(DB_SERIALIZABLE);
        if (transactionAlreadyApplied) {
            if (entry.hasElement("ref")) {
                rollbackRefOp(entry, docsMap, rsSave, gtid);
            } else if (entry.hasElement("ops")) {
                rollbackOps(entry["ops"].Array(), docsMap, rsSave, gtid);
            } else {
                verify(0);
            }
        }
        {
            LOCK_REASON(lockReason, "repl: purging entry from oplog");
            Client::ReadContext ctx(rsoplog, lockReason);
            purgeEntryFromOplog(entry);
        }
        transaction.commit(DB_TXN_NOSYNC);
    }

    void purgeEntryFromOplog(BSONObj entry) {
        Collection* rsOplogDetails = getCollection(rsoplog);
        verify(rsOplogDetails);
        if (entry.hasElement("ref")) {
            OID oid = entry["ref"].OID();
            LOCK_REASON(lockReason, "repl: purging oplog.refs for oplog entry");
            Client::ReadContext ctx(rsOplogRefs, lockReason);
            Client::Transaction txn(DB_SERIALIZABLE);
            deleteIndexRange(
                rsOplogRefs,
                BSON("_id" << BSON("oid" << oid << "seq" << 0)),
                BSON("_id" << BSON("oid" << oid << "seq" << LLONG_MAX)),
                BSON("_id" << 1),
                true,
                false
                );
            txn.commit();
        }

        BSONObj pk = entry["_id"].wrap("");
        const uint64_t flags = Collection::NO_LOCKTREE;
        rsOplogDetails->deleteObject(pk, entry, flags);
    }

    uint64_t expireOplogMilliseconds() {
        const uint32_t days = cmdLine.expireOplogDays;
        const uint32_t hours = days * 24 + cmdLine.expireOplogHours;
        const uint64_t millisPerHour = 3600 * 1000;
        return hours * millisPerHour;
    }

    // assumes we are already in lock and transaction
    static void addOplogPartitionsInLock() {
        Collection* rsOplogDetails = getCollection(rsoplog);
        PartitionedOplogCollection* poc = rsOplogDetails->as<PartitionedOplogCollection>();
        poc->addPartition();

        // try to add a partition to the oplog.refs
        // if the last partition had no insertions, then we cannot
        // add a partition, so it will stay as-is
        Collection* rsOplogRefsDetails = getCollection(rsOplogRefs);
        PartitionedCollection* pc = rsOplogRefsDetails->as<PartitionedCollection>();
        try {
            pc->addPartition();
        }
        catch ( UserException& ue ) {
            if (ue.getCode() == storage::ASSERT_IDS::CapPartitionFailed) {
                // capping the partition failed, likely because there were
                // no spilled transactions in the partition
                // in this case, we just commit the transaction we have
                // and gracefully exit
                return;
            }
            throw;
        }

        // get the metadata of the last partition (which was just added)
        uint64_t opNum = poc->numPartitions();
        verify (opNum > 1); // we just added a partition, so there better be more than 1
        BSONObj opMeta = poc->getPartitionMetadata(opNum-2);
        GTID pivot = getGTIDFromBSON("", opMeta["max"].Obj());
        
        uint64_t refNum = pc->numPartitions();
        verify (refNum > 1); // we just added a partition, so there better be more than 1
        BSONObj refMeta = pc->getPartitionMetadata(refNum-2);

        BSONObjBuilder b;
        b.appendElements(refMeta);
        addGTIDToBSON("maxRefGTID", pivot, b);
        pc->updatePartitionMetadata(refNum-2, b.done());
    }
    
    // adds a partition to oplog and oplog.refs
    void addOplogPartitions() {
        // add a partition to the oplog
        LOCK_REASON(lockReason, "repl: adding partition to oplog");
        Client::WriteContext ctx(rsoplog, lockReason);
        Client::Transaction transaction(DB_SERIALIZABLE);
        addOplogPartitionsInLock();
        transaction.commit();
    }

    // returns the time the last partition was added
    uint64_t getLastPartitionAddTime() {
        LOCK_REASON(lockReason, "repl: getting last time of partition add");
        Client::ReadContext ctx(rsoplog, lockReason);
        Client::Transaction transaction(DB_TXN_SNAPSHOT);
        Collection* rsOplogDetails = getCollection(rsoplog);
        PartitionedOplogCollection* poc = rsOplogDetails->as<PartitionedOplogCollection>();
        uint64_t refNum = poc->numPartitions();
        BSONObj refMeta = poc->getPartitionMetadata(refNum-1);
        BSONElement e = refMeta["createTime"];
        massert(17262, "createTime mysteriously missing from partition metadata", e.ok());
        return e._numberLong();
    }

    static void trimOplogRefs(GTID maxGTID) {
        Lock::assertWriteLocked("local");
        Collection* rsOplogRefsDetails = getCollection(rsOplogRefs);
        PartitionedCollection* pc = rsOplogRefsDetails->as<PartitionedCollection>();
        while (true) {
            uint64_t numPartitions = pc->numPartitions();
            if (numPartitions == 1) {
                // nothing worth trimming
                return;
            }
            BSONObj lastMeta = pc->getPartitionMetadata(0); 
            uint64_t lastID = lastMeta["_id"].Long();
            BSONElement e = lastMeta["maxRefGTID"];
            if (!e.ok()) {
                log() << "unexpectedly, maxRefGTID does not exist, exiting trimOplogRefs" << endl;
                return;
            }
            GTID currGTID = getGTIDFromBSON("maxRefGTID", lastMeta);
            // if the GTID found in maxRefGTID is greater than what we
            // have trimmed to, then we cannot safely drop this partition
            if (GTID::cmp(currGTID, maxGTID) > 0) {
                break;
            }
            pc->dropPartition(lastID);
        }
    }

    static bool willOplogTrimTS(uint64_t tsMillis) {
        // we first grab a read lock to test if any trimming will happen
        LOCK_REASON(lockReason, "repl: checking if we can trim oplog");
        Client::ReadContext ctx(rsoplog, lockReason);
        Client::Transaction transaction(DB_SERIALIZABLE);
        Collection* rsOplogDetails = getCollection(rsoplog);
        PartitionedOplogCollection* poc = rsOplogDetails->as<PartitionedOplogCollection>();
        uint64_t numPartitions = poc->numPartitions();
        verify(numPartitions > 0);
        if (numPartitions == 1) {
            // nothing worth trimming
            return false;
        }
        BSONObj meta = poc->getPartitionMetadata(1);
        // if the createTime of partition 1 is before tsMillis,
        // that means we can drop partition 0, so we would return true
        uint64_t createTime = meta["createTime"]._numberLong();
        transaction.commit();
        LOG(2) << "tsMillis " << tsMillis << " createTime " << createTime << " currTime " << curTimeMillis64() << rsLog;
        return  createTime <= tsMillis;
    }

    // used for trimming an oplog given a timestamp
    void trimOplogWithTS(uint64_t tsMillis) {
        if (!willOplogTrimTS(tsMillis)) {
            LOG(2) << "will not trim with tsMillis " << tsMillis << rsLog;
            return;
        }
        LOCK_REASON(lockReason, "repl: trim oplog with TS");
        Client::WriteContext ctx(rsoplog, lockReason);
        Client::Transaction transaction(DB_SERIALIZABLE);
        Collection* rsOplogDetails = getCollection(rsoplog);
        PartitionedOplogCollection* poc = rsOplogDetails->as<PartitionedOplogCollection>();

        bool oplogTrimmed = false;
        // we have this while loop instead of a conventional for-loop because
        // as we drop partitions, the indexes of the partitions shift
        GTID maxGTIDTrimmed;
        while (true) {
            uint64_t numPartitions = poc->numPartitions();
            if (numPartitions == 1) {
                // nothing worth trimming
                break;
            }
            BSONObj lastMeta = poc->getPartitionMetadata(0); 
            uint64_t lastID = lastMeta["_id"].Long();
            BSONObj meta = poc->getPartitionMetadata(1);
            // if the partitions create time is after tsMillis, we cannot drop
            // the previous partition as the previous partition may also
            // have data that is greater than tsMillis
            uint64_t createTime = meta["createTime"]._numberLong();
            if (createTime > tsMillis) {
                break;
            }
            maxGTIDTrimmed = getGTIDFromBSON("", lastMeta["max"].Obj());
            poc->dropPartition(lastID);
            oplogTrimmed = true;
        }

        // only try to trim oplog.refs if we successfully trimmed oplog
        if (oplogTrimmed) {
            trimOplogRefs(maxGTIDTrimmed);
        }
        transaction.commit();
    }

    static bool willOplogTrimGTID(GTID gtid) {
        // we first grab a read lock to test if any trimming will happen
        LOCK_REASON(lockReason, "repl: checking if we can trim oplog");
        Client::ReadContext ctx(rsoplog, lockReason);
        Client::Transaction transaction(DB_SERIALIZABLE);
        Collection* rsOplogDetails = getCollection(rsoplog);
        PartitionedOplogCollection* poc = rsOplogDetails->as<PartitionedOplogCollection>();
        uint64_t numPartitions = poc->numPartitions();
        verify(numPartitions > 0);
        if (numPartitions == 1) {
            // nothing worth trimming
            return false;
        }
        BSONObj meta = poc->getPartitionMetadata(0);
        // if the GTID of partition 0 is less than gtid,
        // that means we can drop partition 0, so we would return true
        GTID pivot = getGTIDFromBSON("", meta["max"].Obj());
        transaction.commit();
        return GTID::cmp(pivot, gtid) <= 0;
    }

    void trimOplogwithGTID(GTID gtid) {
        if (!willOplogTrimGTID(gtid)) {
            return;
        }
        LOCK_REASON(lockReason, "repl: trim oplog with GTID");
        Client::WriteContext ctx(rsoplog, lockReason);
        Client::Transaction transaction(DB_SERIALIZABLE);
        Collection* rsOplogDetails = getCollection(rsoplog);
        PartitionedOplogCollection* poc = rsOplogDetails->as<PartitionedOplogCollection>();

        bool oplogTrimmed = false;
        // we have this while loop instead of a conventional for-loop because
        // as we drop partitions, the indexes of the partitions shift
        GTID maxGTIDTrimmed;
        while (true) {
            uint64_t numPartitions = poc->numPartitions();
            if (numPartitions == 1) {
                // nothing worth trimming
                break;
            }
            uint64_t lastID = poc->getPartitionMetadata(0)["_id"].Long();
            BSONObj meta = poc->getPartitionMetadata(0);
            GTID pivot = getGTIDFromBSON("", meta["max"].Obj());
            // if the partitions pivot is greater than GTID, we cannot drop
            // the partition
            if (GTID::cmp(pivot, gtid) > 0) {
                break;
            }
            poc->dropPartition(lastID);
            maxGTIDTrimmed = pivot;
            oplogTrimmed = true;
        }

        // only try to trim oplog.refs if we successfully trimmed oplog
        if (oplogTrimmed) {
            trimOplogRefs(maxGTIDTrimmed);
        }
        transaction.commit();
    }

    void convertOplogToPartitionedIfNecessary() {
        LOCK_REASON(lockReason, "repl: maybe convert oplog to partitioned on startup");
        Client::WriteContext ctx(rsoplog, lockReason);
        Client::Transaction transaction(DB_SERIALIZABLE);
        bool shouldAddPartition = false;
        Collection* rsOplogDetails = getCollection(rsoplog);
        if (!rsOplogDetails->isPartitioned()) {
            log() << "Oplog was not partitioned, converting it to a partitioned collection" << rsLog;
            convertToPartitionedCollection(rsoplog);
            shouldAddPartition = true;
        }
        Collection* rsOplogRefsDetails = getCollection(rsOplogRefs);
        if (!rsOplogRefsDetails->isPartitioned()) {
            log() << "Oplog.refs was not partitioned, converting it to a partitioned collection" << rsLog;
            convertToPartitionedCollection(rsOplogRefs);
        }
        //
        // if we converted the oplog from partitioned to non-partitioned,
        // then we ought to immedietely add a partition
        // Because we did the conversion, we could not have
        // done it on an empty oplog, so there should be no issues
        // adding a partition
        //
        // The benefit here is that all work done before upgrade
        // is in a separate partition and can be dropped sooner
        // rather than later (which we want, because odds are this
        // partition is big
        //
        if (shouldAddPartition) {
            log() << "auto adding a partition after upgrade" << rsLog;
            addOplogPartitionsInLock();
        }
        transaction.commit();
    }

    void RollbackSaveData::initialize() {
        // write lock needed to (maybe) drop and recreate the
        // collection
        LOCK_REASON(lockReason, "repl: initializing set of committed GTIDs during rollback");
        Client::WriteContext ctx(rsRollbackOpdata, lockReason);
        Client::Transaction txn(DB_SERIALIZABLE);
        string errmsg;
        Collection* cl = getCollection(rsRollbackOpdata);
        if (cl == NULL) {
            bool ret = userCreateNS(rsRollbackOpdata, BSONObj(), errmsg, false);
            verify(ret);
            cl = getCollection(rsRollbackOpdata);
            verify(cl);
        }
        // now we have the collection available, let's figure out what _rollbackID is
        shared_ptr<Cursor> c(Cursor::make(cl, -1));
        if (c->ok()) {
            BSONObj last = c->current();
            BSONObj lastIDObj = last["_id"].Obj();
            uint64_t lastID = lastIDObj["rid"].numberLong();
            _rollbackID = lastID+1;
        }
        else {
            _rollbackID = 0;
        }
        txn.commit();
    }

    void RollbackSaveData::saveOp(GTID gtid, const BSONObj& op) {
        LOCK_REASON(lockReason, "repl: writing data to rollback.opdata");
        Client::ReadContext ctx(rsRollbackOpdata, lockReason);
        Collection* cl = getCollection(rsRollbackOpdata);
        verify(cl);
    
        BSONObjBuilder docBuilder;
        BSONObjBuilder idBuilder(docBuilder.subobjStart("_id"));
        idBuilder.append("rid", _rollbackID);
        idBuilder.append("seq", _seq);
        idBuilder.done();
        docBuilder.append("gtid", gtid);
        docBuilder.append("gtidString", gtid.toString());
        docBuilder.appendDate("time", curTimeMillis64());
        docBuilder.append("op", op);
    
        const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
        BSONObj doc = docBuilder.obj();
        cl->insertObject(doc, flags);
    
        _seq++;
    }

}
