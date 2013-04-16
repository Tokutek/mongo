// @file oplog.cpp

/**
*    Copyright (C) 2008 10gen Inc.
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
#include "oplog.h"
#include "repl_block.h"
#include "repl.h"
#include "commands.h"
#include "repl/rs.h"
#include "stats/counters.h"
#include "../util/file.h"
#include "../util/startup_test.h"
#include "queryoptimizer.h"
#include "ops/update.h"
#include "ops/delete.h"
#include "mongo/db/instance.h"
#include "mongo/db/repl/bgsync.h"
#include "mongo/db/db_flags.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/jsobjmanipulator.h"

namespace mongo {

    // from d_migrate.cpp
    void logOpForSharding( const char * opstr , const char * ns , const BSONObj& obj , BSONObj * patt );

    int __findingStartInitialTimeout = 5; // configurable for testing

    // cached copies of these...so don't rename them, drop them, etc.!!!
    static Database *localDB = 0;
    static NamespaceDetails *rsOplogDetails = NULL;
    static NamespaceDetails *replInfoDetails = NULL;
    
    void oplogCheckCloseDatabase( Database * db ) {
        verify( Lock::isW() );
        localDB = 0;
        rsOplogDetails = NULL;
        replInfoDetails = NULL;
        resetSlaveCache();
    }

    static void _logOpUninitialized(GTID gtid, uint64_t timestamp, uint64_t hash, BSONArray& opInfo) {
        log() << "WHAT IS GOING ON???????? " << endl;
    }

    void deleteOplogFiles() {
        Lock::DBWrite lk1("local");
        localDB = NULL;
        rsOplogDetails = NULL;
        replInfoDetails = NULL;
        
        Client::Context ctx( rsoplog, dbpath, false);
        // TODO: code review this for possible error cases
        // although, I don't think we care about error cases,
        // just that after we exit, oplog files don't exist
        BSONObjBuilder out;
        string errmsg;
        dropCollection(rsoplog, errmsg, out);
        BSONObjBuilder out2;
        string errmsg2;
        dropCollection(rsReplInfo, errmsg, out);
    }

    void openOplogFiles() {
        Lock::DBWrite lk1("local");
        const char *logns = rsoplog;
        if ( rsOplogDetails == 0 ) {
            Client::Context ctx( logns , dbpath, false);
            localDB = ctx.db();
            verify( localDB );
            rsOplogDetails = nsdetails(logns);
            massert(13347, "local.oplog.rs missing. did you drop it? if so restart server", rsOplogDetails);
        }
        if (replInfoDetails == NULL) {
            Client::Context ctx( rsReplInfo , dbpath, false);
            replInfoDetails = nsdetails(rsReplInfo);
            massert(16747, "local.replInfo missing. did you drop it? if so restart server", replInfoDetails);
        }
    }
    
    static void _logTransactionOps(GTID gtid, uint64_t timestamp, uint64_t hash, BSONArray& opInfo) {
        Lock::DBRead lk1("local");
        mutex::scoped_lock lk2(OpTime::m);

        BSONObjBuilder b;
        addGTIDToBSON("_id", gtid, b);
        b.appendTimestamp("ts", timestamp);
        b.append("h", (long long)hash);
        b.append("a", true);
        b.append("ops", opInfo);

        BSONObj bb = b.done();
        writeEntryToOplog(bb);
    }

    void logToReplInfo(GTID minLiveGTID, GTID minUnappliedGTID) {
        Lock::DBRead lk("local");
        BufBuilder bufbuilder(256);
        BSONObjBuilder b(bufbuilder);
        b.append("_id", "minLive");
        addGTIDToBSON("GTID", minLiveGTID, b);
        BSONObj bb = b.done();
        uint64_t flags = (ND_UNIQUE_CHECKS_OFF | ND_LOCK_TREE_OFF);
        replInfoDetails->insertObject(bb, flags);

        bufbuilder.reset();
        BSONObjBuilder b2(bufbuilder);
        b2.append("_id", "minUnapplied");
        addGTIDToBSON("GTID", minUnappliedGTID, b2);
        BSONObj bb2 = b2.done();
        replInfoDetails->insertObject(bb2, flags);
    }
    
    static void (*_logTransactionOp)(GTID gtid, uint64_t timestamp, uint64_t hash, BSONArray& opInfo) = _logOpUninitialized;
    // TODO: (Zardosht) hopefully remove these two phases
    void newReplUp() {
        _logTransactionOp = _logTransactionOps;
    }
    void newRepl() {
        _logTransactionOp = _logTransactionOps;
    }

    void logTransactionOps(GTID gtid, uint64_t timestamp, uint64_t hash, BSONArray& opInfo) {
        _logTransactionOp(gtid, timestamp, hash, opInfo);
        // TODO: Figure out for sharding
        //logOpForSharding( opstr , ns , obj , patt );
    }

    void createOplog() {
        Lock::GlobalWrite lk;
        bool rs = !cmdLine._replSet.empty();
        verify(rs);
        
        const char * oplogNS = rsoplog;
        const char * replInfoNS = rsReplInfo;
        Client::Context ctx(oplogNS);
        NamespaceDetails * oplogNSD = nsdetails(oplogNS);
        NamespaceDetails * replInfoNSD = nsdetails(replInfoNS);
        if (oplogNSD || replInfoNSD) {
            // TODO: (Zardosht), figure out if there are any checks to do here
            // not sure under what scenarios we can be here, so
            // making a printf to catch this so we can investigate
            tokulog() << "createOplog called with existing collections, investigate why.\n" << endl;
            return;
        }

        /* create an oplog collection, if it doesn't yet exist. */
        BSONObjBuilder b;

        log() << "******" << endl;
        log() << "creating replication oplog." << endl;
        log() << "TODO: FIGURE OUT SIZE!!!." << endl;
        // create the namespace
        string err;
        BSONObj o = b.done();
        bool ret = userCreateNS(oplogNS, o, err, false);
        verify(ret);
        ret = userCreateNS(replInfoNS, o, err, false);
        verify(ret);
        log() << "******" << endl;
    }

    GTID getGTIDFromOplogEntry(BSONObj o) {
        return getGTIDFromBSON("_id", o);
    }

    bool getLastGTIDinOplog(GTID* gtid) {
        Lock::DBRead lk(rsoplog);
        BSONObj o;
        if( Helpers::getLast(rsoplog, o) ) {
            *gtid = getGTIDFromOplogEntry(o);
            return true;
        }
        return false;
    }

    bool gtidExistsInOplog(GTID gtid) {
        Lock::DBRead lk(rsoplog);
        char gtidBin[GTID::GTIDBinarySize()];
        gtid.serializeBinaryData(gtidBin);
        BSONObj result;
        BSONObj query(BSON("_id" << gtidBin));
        bool found = Helpers::findOne(
            rsoplog, 
            query, 
            result
            );
        return found;
    }

    void writeEntryToOplog(BSONObj entry) {
        verify(rsOplogDetails);
        uint64_t flags = (ND_UNIQUE_CHECKS_OFF | ND_LOCK_TREE_OFF);
        rsOplogDetails->insertObject(entry, flags);
    }

    // takes an entry that was written _logTransactionOps
    // and applies them to collections
    void applyTransactionFromOplog(BSONObj entry) {
        bool transactionAlreadyApplied = entry["a"].Bool();
        if (!transactionAlreadyApplied) {
            std::vector<BSONElement> ops = entry["ops"].Array();
            const size_t numOps = ops.size();

            for(size_t i = 0; i < numOps; ++i) {
                BSONElement* curr = &ops[i];
                OpLogHelpers::applyOperationFromOplog(curr->Obj());
            }
            // set the applied bool to false, to let the oplog know that
            // this entry has not been applied to collections
            BSONElementManipulator(entry["a"]).setBool(true);
            writeEntryToOplog(entry);
        }
    }

    // -------------------------------------
    void Sync::setHostname(const string& hostname) {
        hn = hostname;
    }

    BSONObj Sync::getMissingDoc(const BSONObj& o) {
        OplogReader missingObjReader;
        const char *ns = o.getStringField("ns");

        // capped collections
        NamespaceDetails *nsd = nsdetails(ns);
        if ( nsd && nsd->isCapped() ) {
            log() << "replication missing doc, but this is okay for a capped collection (" << ns << ")" << endl;
            return BSONObj();
        }

        uassert(15916, str::stream() << "Can no longer connect to initial sync source: " << hn, missingObjReader.connect(hn));

        // might be more than just _id in the update criteria
        BSONObj query = BSONObjBuilder().append(o.getObjectField("o2")["_id"]).obj();
        BSONObj missingObj;
        try {
            missingObj = missingObjReader.findOne(ns, query);
        } catch(DBException& e) {
            log() << "replication assertion fetching missing object: " << e.what() << endl;
            throw;
        }

        return missingObj;
    }

    bool Sync::shouldRetry(const BSONObj& o) {
        // should already have write lock
        const char *ns = o.getStringField("ns");
        Client::Context ctx(ns);

        // we don't have the object yet, which is possible on initial sync.  get it.
        log() << "replication info adding missing object" << endl; // rare enough we can log

        BSONObj missingObj = getMissingDoc(o);

        if( missingObj.isEmpty() ) {
            log() << "replication missing object not found on source. presumably deleted later in oplog" << endl;
            log() << "replication o2: " << o.getObjectField("o2").toString() << endl;
            log() << "replication o firstfield: " << o.getObjectField("o").firstElementFieldName() << endl;

            return false;
        }
        else {
            ::abort(); //DiskLoc d = //theDataFileMgr.insert(ns, (void*) missingObj.objdata(), missingObj.objsize());
            //uassert(15917, "Got bad disk location when attempting to insert", !d.isNull());

            LOG(1) << "replication inserted missing doc: " << missingObj.toString() << endl;
            return true;
        }
    }
}
