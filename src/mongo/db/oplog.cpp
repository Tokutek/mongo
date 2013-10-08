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

#include "mongo/db/oplog.h"
#include "mongo/db/cmdline.h"
#include "mongo/db/repl_block.h"
#include "mongo/db/repl.h"
#include "mongo/db/commands.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/query_optimizer_internal.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/instance.h"
#include "mongo/db/repl/bgsync.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/jsobjmanipulator.h"
#include "mongo/util/elapsed_tracker.h"

namespace mongo {

    // cached copies of these...so don't rename them, drop them, etc.!!!
    static NamespaceDetails *rsOplogDetails = NULL;
    static NamespaceDetails *rsOplogRefsDetails = NULL;
    static NamespaceDetails *replInfoDetails = NULL;
    
    void deleteOplogFiles() {
        rsOplogDetails = NULL;
        rsOplogRefsDetails = NULL;
        replInfoDetails = NULL;
        
        Client::Context ctx(rsoplog, dbpath);
        // TODO: code review this for possible error cases
        // although, I don't think we care about error cases,
        // just that after we exit, oplog files don't exist
        BSONObjBuilder out;
        string errmsg;
        dropCollection(rsoplog, errmsg, out);
        dropCollection(rsOplogRefs, errmsg, out);
        dropCollection(rsReplInfo, errmsg, out);
    }

    void openOplogFiles() {
        const char *logns = rsoplog;
        if (rsOplogDetails == NULL) {
            Client::Context ctx(logns , dbpath);
            rsOplogDetails = nsdetails(logns);
            massert(13347, "local.oplog.rs missing. did you drop it? if so restart server", rsOplogDetails);
        }
        if (rsOplogRefsDetails == NULL) {
            Client::Context ctx(rsOplogRefs , dbpath);
            rsOplogRefsDetails = nsdetails(rsOplogRefs);
            massert(16814, "local.oplog.refs missing. did you drop it? if so restart server", rsOplogRefsDetails);
        }
        if (replInfoDetails == NULL) {
            Client::Context ctx(rsReplInfo , dbpath);
            replInfoDetails = nsdetails(rsReplInfo);
            massert(16747, "local.replInfo missing. did you drop it? if so restart server", replInfoDetails);
        }
    }
    
    static void _logTransactionOps(GTID gtid, uint64_t timestamp, uint64_t hash, BSONArray& opInfo) {
        Lock::DBRead lk1("local");

        BSONObjBuilder b;
        addGTIDToBSON("_id", gtid, b);
        b.appendDate("ts", timestamp);
        b.append("h", (long long)hash);
        b.append("a", true);
        b.append("ops", opInfo);

        BSONObj bb = b.done();
        // write it to oplog
        LOG(3) << "writing " << bb.toString(false, true) << " to master " << endl;
        writeEntryToOplog(bb);
    }

    // assumes it is locked on entry
    void logToReplInfo(GTID minLiveGTID, GTID minUnappliedGTID) {
        BufBuilder bufbuilder(256);
        BSONObjBuilder b(bufbuilder);
        b.append("_id", "minLive");
        addGTIDToBSON("GTID", minLiveGTID, b);
        BSONObj bb = b.done();
        uint64_t flags = (NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE);
        replInfoDetails->insertObject(bb, flags);

        bufbuilder.reset();
        BSONObjBuilder b2(bufbuilder);
        b2.append("_id", "minUnapplied");
        addGTIDToBSON("GTID", minUnappliedGTID, b2);
        BSONObj bb2 = b2.done();
        replInfoDetails->insertObject(bb2, flags);
    }
    
    void logTransactionOps(GTID gtid, uint64_t timestamp, uint64_t hash, BSONArray& opInfo) {
        _logTransactionOps(gtid, timestamp, hash, opInfo);
    }

    void logTransactionOpsRef(GTID gtid, uint64_t timestamp, uint64_t hash, OID& oid) {
        Lock::DBRead lk1("local");
        BSONObjBuilder b;
        addGTIDToBSON("_id", gtid, b);
        b.appendDate("ts", timestamp);
        b.append("h", (long long)hash);
        b.append("a", true);
        b.append("ref", oid);
        BSONObj bb = b.done();
        writeEntryToOplog(bb);
    }

    void logOpsToOplogRef(BSONObj o) {
        Lock::DBRead lk("local");
        writeEntryToOplogRefs(o);
    }

    void createOplog() {
        bool rs = !cmdLine._replSet.empty();
        verify(rs);
        
        const char * oplogNS = rsoplog;
        const char * replInfoNS = rsReplInfo;
        Client::Context ctx(oplogNS);
        NamespaceDetails * oplogNSD = nsdetails(oplogNS);
        NamespaceDetails * oplogRefsNSD = nsdetails(rsOplogRefs);        
        NamespaceDetails * replInfoNSD = nsdetails(replInfoNS);
        if (oplogNSD || replInfoNSD || oplogRefsNSD) {
            // TODO: (Zardosht), figure out if there are any checks to do here
            // not sure under what scenarios we can be here, so
            // making a printf to catch this so we can investigate
            tokulog() << "createOplog called with existing collections, investigate why.\n" << endl;
            return;
        }

        /* create an oplog collection, if it doesn't yet exist. */
        BSONObjBuilder b;

        // create the namespace
        string err;
        BSONObj o = b.done();
        bool ret;
        ret = userCreateNS(oplogNS, o, err, false);
        verify(ret);
        ret = userCreateNS(rsOplogRefs, o, err, false);
        verify(ret);
        ret = userCreateNS(replInfoNS, o, err, false);
        verify(ret);
    }

    GTID getGTIDFromOplogEntry(BSONObj o) {
        return getGTIDFromBSON("_id", o);
    }

    bool getLastGTIDinOplog(GTID* gtid) {
        Client::ReadContext ctx(rsoplog);
        // TODO: Should this be using rsOplogDetails, verifying non-null?
        NamespaceDetails *d = nsdetails(rsoplog);
        shared_ptr<Cursor> c( BasicCursor::make(d, -1) );
        if (c->ok()) {
            *gtid = getGTIDFromOplogEntry(c->current());
            return true;
        }
        return false;
    }

    bool gtidExistsInOplog(GTID gtid) {
        Client::ReadContext ctx(rsoplog);
        // TODO: Should this be using rsOplogDetails, verifying non-null?
        NamespaceDetails *d = nsdetails(rsoplog);
        BSONObjBuilder q;
        BSONObj result;
        addGTIDToBSON("_id", gtid, q);
        const bool found = d != NULL &&
            d->findOne(
               q.done(),
               result
               );
        return found;
    }

    void writeEntryToOplog(BSONObj entry) {
        verify(rsOplogDetails);
        uint64_t flags = (NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE);
        rsOplogDetails->insertObject(entry, flags);
    }

    void writeEntryToOplogRefs(BSONObj o) {
        verify(rsOplogRefsDetails);
        uint64_t flags = (NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE);
        rsOplogRefsDetails->insertObject(o, flags);
    }

    // assumes oplog is read locked on entry
    void replicateTransactionToOplog(BSONObj& op) {
        // set the applied bool to false, to let the oplog know that
        // this entry has not been applied to collections
        BSONElementManipulator(op["a"]).setBool(false);
        writeEntryToOplog(op);
    }

    // Copy a range of documents to the local oplog.refs collection
    static void copyOplogRefsRange(OplogReader &r, OID oid) {
        shared_ptr<DBClientCursor> c = r.getOplogRefsCursor(oid);
        Client::ReadContext ctx(rsOplogRefs);
        while (c->more()) {
            BSONObj b = c->next();
            BSONElement eOID = b.getFieldDotted("_id.oid");
            if (oid != eOID.OID()) {
                break;
            }
            LOG(6) << "copyOplogRefsRange " << b << endl;
            writeEntryToOplogRefs(b);
        }
    }

    void replicateFullTransactionToOplog(BSONObj& o, OplogReader& r, bool* bigTxn) {
        *bigTxn = false;
        if (o.hasElement("ref")) {
            OID oid = o["ref"].OID();
            LOG(3) << "oplog ref " << oid << endl;
            copyOplogRefsRange(r, oid);
            *bigTxn = true;
        }

        Client::ReadContext ctx(rsoplog);
        replicateTransactionToOplog(o);
    }

    // apply all operations in the array
    void applyOps(std::vector<BSONElement> ops) {
        const size_t numOps = ops.size();
        for(size_t i = 0; i < numOps; ++i) {
            BSONElement* curr = &ops[i];
            OpLogHelpers::applyOperationFromOplog(curr->Obj());
        }
    }

    // find all oplog entries for a given OID in the oplog.refs collection and apply them
    // TODO this should be a range query on oplog.refs where _id.oid == oid and applyOps to
    // each entry found.  The locking of the query interleaved with the locking in the applyOps
    // did not work, so it a sequence of point queries.  
    // TODO verify that the query plan is a indexed lookup.
    // TODO verify that the query plan does not fetch too many docs and then only process one of them.
    void applyRefOp(BSONObj entry) {
        OID oid = entry["ref"].OID();
        LOG(3) << "apply ref " << entry << " oid " << oid << endl;
        long long seq = 0; // note that 0 is smaller than any of the seq numbers
        while (1) {
            BSONObj entry;
            {
                Client::ReadContext ctx(rsOplogRefs);
                // TODO: Should this be using rsOplogRefsDetails, verifying non-null?
                NamespaceDetails *d = nsdetails(rsOplogRefs);
                if (d == NULL || !d->findOne(BSON("_id" << BSON("$gt" << BSON("oid" << oid << "seq" << seq))), entry, true)) {
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
            applyOps(entry["ops"].Array());
        }
    }
    
    // takes an entry that was written _logTransactionOps
    // and applies them to collections
    //
    // TODO: possibly improve performance of this. We create and destroy a
    // context for each operation. Find a way to amortize it out if necessary
    //
    void applyTransactionFromOplog(BSONObj entry) {
        bool transactionAlreadyApplied = entry["a"].Bool();
        if (!transactionAlreadyApplied) {
            Client::Transaction transaction(DB_SERIALIZABLE);
            if (entry.hasElement("ref")) {
                applyRefOp(entry);
            } else if (entry.hasElement("ops")) {
                applyOps(entry["ops"].Array());
            } else {
                verify(0);
            }
            // set the applied bool to false, to let the oplog know that
            // this entry has not been applied to collections
            BSONElementManipulator(entry["a"]).setBool(true);
            {
                Lock::DBRead lk1("local");
                writeEntryToOplog(entry);
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
                log() << "exception during commit of applyTransactionFromOplog, aborting system: " << e.what() << endl;
                printStackTrace();
                logflush();
                ::abort();
            }
        }
    }
    
    // apply all operations in the array
    void rollbackOps(std::vector<BSONElement> ops) {
        const size_t numOps = ops.size();
        for(size_t i = 0; i < numOps; ++i) {
            // note that we have to rollback the transaction backwards
            BSONElement* curr = &ops[numOps - i - 1];
            OpLogHelpers::rollbackOperationFromOplog(curr->Obj());
        }
    }

    void rollbackRefOp(BSONObj entry) {
        OID oid = entry["ref"].OID();
        LOG(3) << "rollback ref " << entry << " oid " << oid << endl;
        long long seq = LLONG_MAX;
        while (1) {
            BSONObj entry;
            {
                Client::ReadContext ctx(rsOplogRefs);
                // TODO: Should this be using rsOplogRefsDetails, verifying non-null?
                NamespaceDetails *d = nsdetails(rsOplogRefs);
                if (d == NULL || !d->findOne(BSON("_id" << BSON("$lt" << BSON("oid" << oid << "seq" << seq))), entry, true)) {
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
            rollbackOps(entry["ops"].Array());
        }
    }

    void rollbackTransactionFromOplog(BSONObj entry) {
        bool transactionAlreadyApplied = entry["a"].Bool();
        Client::Transaction transaction(DB_SERIALIZABLE);
        if (transactionAlreadyApplied) {
            if (entry.hasElement("ref")) {
                rollbackRefOp(entry);
            } else if (entry.hasElement("ops")) {
                rollbackOps(entry["ops"].Array());
            } else {
                verify(0);
            }
        }
        {
            Lock::DBRead lk1("local");
            purgeEntryFromOplog(entry);
        }
        transaction.commit(DB_TXN_NOSYNC);
    }
    
    void purgeEntryFromOplog(BSONObj entry) {
        verify(rsOplogDetails);
        if (entry.hasElement("ref")) {
            OID oid = entry["ref"].OID();
            Helpers::removeRange(
                rsOplogRefs,
                BSON("_id" << BSON("oid" << oid << "seq" << minKey)),
                BSON("_id" << BSON("oid" << oid << "seq" << maxKey)),
                BSON("_id" << 1),
                true,
                false
                );
        }

        BSONObj pk = entry["_id"].wrap("");
        uint64_t flags = (NamespaceDetails::NO_LOCKTREE);
        rsOplogDetails->deleteObject(pk, entry, flags);
    }

    uint64_t expireOplogMilliseconds() {
        const uint32_t days = cmdLine.expireOplogDays;
        const uint32_t hours = days * 24 + cmdLine.expireOplogHours;
        const uint64_t millisPerHour = 3600 * 1000;
        return hours * millisPerHour;
    }

    void hotOptimizeOplogTo(GTID gtid) {
        Client::ReadContext ctx(rsoplog);

        // do a hot optimize up until gtid;
        BSONObjBuilder q;
        addGTIDToBSON("", gtid, q);
        rsOplogDetails->optimizePK(minKey, q.done());
    }
}
