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

#include "mongo/pch.h"

#include "mongo/db/client.h"
#include "mongo/db/commands/fsync.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/db/crash.h"
#include "mongo/db/repl/bgsync.h"
#include "mongo/db/repl/rs_sync.h"
#include "mongo/base/counter.h"
#include "mongo/db/stats/timer_stats.h"
#include "mongo/client/remote_transaction.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/insert.h"

namespace mongo {

    #define ROLLBACK_ID "rollbackStatus"

    void incRBID();
    void applyMissingOpsInOplog(GTID minUnappliedGTID, const bool inRollback);

    // this enum represents possible values stored in local.replInfo during
    // rollback, under the "rollbackStatus" document. The existence of this
    // document implies that a rollback is in progress. It's possible
    // to resume a rollback that is in some of these states. We save this
    // state because we don't want to depend on rollback succeeding in "one shot".
    // A lot can go wrong, like a connection dying.
    enum rollback_state {
        RB_NOT_STARTED = 0, // this means that we have not started a rollback
        RB_STARTING, // means we have started rolling back oplog entries. We cannot recover from this state
        RB_DOCS_REMOVED, // documents have been removed, but a remove snapshot's version has not been applied
        RB_SNAPSHOT_APPLIED // remote snapshot of documents has been applied, but we have yet to sync forward to a safe point
        // After syncing forward, via replicateForwardToGTID, we are done with rollback
    };

    static BSONObj findOneFromConn(DBClientConnection* conn, const string &ns, const Query& query) {
        return conn->findOne(ns, query, 0, QueryOption_SlaveOk);
    }

    // assumes transaction is created
    void updateRollbackStatus(const BSONObj& status) {
        BSONObj o = status.getOwned();
        LOCK_REASON(lockReason, "repl rollback: updating rollback status to replInfo");
        Client::ReadContext ctx(rsReplInfo, lockReason);
        Collection* replInfoDetails = getCollection(rsReplInfo);
        verify(replInfoDetails);
        const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
        replInfoDetails->insertObject(o, flags);
    }

    bool getRollbackStatus(BSONObj& o) {
        LOCK_REASON(lockReason, "repl rollback: getting rollback status from replInfo");
        Client::ReadContext ctx(rsReplInfo, lockReason);
        Client::Transaction txn(DB_READ_UNCOMMITTED);
        bool found = Collection::findOne(rsReplInfo, BSON("_id" << ROLLBACK_ID), o, true);
        txn.commit();
        return found;
    }

    void clearRollbackStatus(const BSONObj& status) {
        LOCK_REASON(lockReason, "repl rollback: clearing rollback status");
        Client::ReadContext ctx(rsReplInfo, lockReason);
        Collection* cl= getCollection(rsReplInfo);
        verify(cl);
        cl->deleteObject(BSON("" << ROLLBACK_ID), status);
    }

    bool isRollbackRequired(OplogReader& r, uint64_t *lastTS) {
        string hn = r.conn()->getServerAddress();
        verify(r.more());
        BSONObj rollbackStatus;
        bool found = getRollbackStatus(rollbackStatus);
        if (found) {
            // we have a rollback in progress,
            // must complete it
            log() << "Rollback needed, found rollbackStatus: " << rollbackStatus << rsLog;
            return true;
        }

        BSONObj o = r.nextSafe();
        uint64_t ts = o["ts"]._numberLong();
        uint64_t lastHash = o["h"].numberLong();
        GTID gtid = getGTIDFromBSON("_id", o);

        if (!theReplSet->gtidManager->rollbackNeeded(gtid, ts, lastHash)) {
            log() << "Rollback NOT needed! " << gtid << endl;
            return false;
        }

        log() << "Rollback needed! Our GTID" <<
            theReplSet->gtidManager->getLiveState().toString() <<
            " remote GTID: " << gtid.toString() << ". Attempting rollback." << rsLog;

        *lastTS = ts;
        return true;
    }

    void findRollbackPoint(
        OplogReader& r, uint64_t oplogTS,
        GTID* idToRollbackTo,
        uint64_t* rollbackPointTS,
        uint64_t* rollbackPointHash
        )
    {
        bool gtidFound = false;
        try {
            GTID ourLast = theReplSet->gtidManager->getLiveState();
            shared_ptr<DBClientCursor> rollbackCursor = r.getRollbackCursor(ourLast);
            uassert(17350, "rollback failed to get a cursor to start reading backwards from.", rollbackCursor.get());
            while (rollbackCursor->more()) {
                BSONObj remoteObj = rollbackCursor->next();
                GTID remoteGTID = getGTIDFromBSON("_id", remoteObj);
                uint64_t remoteTS = remoteObj["ts"]._numberLong();
                uint64_t remoteLastHash = remoteObj["h"].numberLong();
                if (remoteTS + 1800*1000 < oplogTS) {
                    log() << "Rollback takes us too far back, throwing exception. remoteTS: " << remoteTS << " oplogTS: " << oplogTS << rsLog;
                    throw RollbackOplogException("replSet rollback too long a time period for a rollback (at least 30 minutes).");
                    break;
                }
                //now try to find an entry in our oplog with that GTID
                BSONObjBuilder localQuery;
                BSONObj localObj;
                addGTIDToBSON("_id", remoteGTID, localQuery);
                bool foundLocally = false;
                {
                    LOCK_REASON(lockReason, "repl: looking up oplog entry for rollback");
                    Client::ReadContext ctx(rsoplog, lockReason);
                    Client::Transaction transaction(DB_SERIALIZABLE);
                    foundLocally = Collection::findOne(rsoplog, localQuery.done(), localObj);
                    transaction.commit();
                }
                if (foundLocally) {
                    GTID localGTID = getGTIDFromBSON("_id", localObj);
                    uint64_t localTS = localObj["ts"]._numberLong();
                    uint64_t localLastHash = localObj["h"].numberLong();
                    if (localLastHash == remoteLastHash &&
                        localTS == remoteTS &&
                        GTID::cmp(localGTID, remoteGTID) == 0
                        )
                    {
                        *idToRollbackTo = localGTID;
                        *rollbackPointTS = localTS;
                        *rollbackPointHash = localLastHash;
                        gtidFound = true;
                        log() << "found id to rollback to " << idToRollbackTo->toString() << rsLog;
                        break;
                    }
                }
            }
            // At this point, either we have found the point to try to rollback to,
            // or we have determined that we cannot rollback
            if (!gtidFound) {
                // we cannot rollback
                throw RollbackOplogException("could not find ID to rollback to");
            }
        }
        catch (DBException& e) {
            log() << "Caught DBException during rollback " << e.toString() << rsLog;
            throw RollbackOplogException("DBException while trying to find ID to rollback to: " + e.toString());
        }
        catch (std::exception& e2) {
            log() << "Caught std::exception during rollback " << e2.what() << rsLog;
            throw RollbackOplogException(str::stream() << "Exception while trying to find ID to rollback to: " << e2.what());
        }
    }

    bool canStartRollback(OplogReader& r, GTID idToRollbackTo) {
        shared_ptr<DBClientConnection> conn(r.conn_shared());
        // before we start rollback, let's make sure that the minUnapplied on the remote
        // server is past the id that we are rolling back to. Otherwise, the snapshot
        // we create will not be up to date, and the rollback algorithm will not work
        BSONObjBuilder b;
        b.append("_id", "minUnapplied");
        // Note that another way to get this information is to
        // request a heartbeat. That one will technically return
        // a more up to date value for minUnapplied
        BSONObj res = findOneFromConn(conn.get(), rsReplInfo, Query(b.done()));
        GTID minUnapplied = getGTIDFromBSON("GTID", res);
        if (GTID::cmp(minUnapplied, idToRollbackTo) < 0) {
            log() << "Remote server has minUnapplied " << minUnapplied.toString() << \
                " we want to rollback to " << idToRollbackTo.toString() << \
                ". Therefore, exiting and retrying." << rsLog;
            return false;
        }
        return true;
    }

    // transitions us to rollback state,
    // writes to replInfo fact that we have started rollback
    void startRollback(
        GTID idToRollbackTo,
        uint64_t rollbackPointTS,
        uint64_t rollbackPointHash
        ) 
    {
        incRBID();
        // now that we are settled, we have to take care of the GTIDManager
        // and the repl info thread.
        // We need to reset the state of the GTIDManager to the point
        // we intend to rollback to, and we need to make sure that the repl info thread
        // has captured this information.
        theReplSet->gtidManager->resetAfterInitialSync(
            idToRollbackTo,
            rollbackPointTS,
            rollbackPointHash
            );
        // now force an update of the repl info thread
        theReplSet->forceUpdateReplInfo();
        Client::Transaction txn(DB_SERIALIZABLE);
        updateRollbackStatus(BSON("_id" << ROLLBACK_ID << "state" << RB_STARTING<< "info" << "starting rollback"));
        txn.commit(DB_TXN_NOSYNC);
    }

    void rollbackToGTID(GTID idToRollbackTo, RollbackDocsMap* docsMap, RollbackSaveData* rsSave) {
        // at this point, everything should be settled, the applier should
        // have nothing left (and remain that way, because this is the only
        // thread that can put work on the applier). Now we can rollback
        // the data.
        while (true) {
            BSONObj o;
            {
                LOCK_REASON(lockReason, "repl: checking for oplog data");
                Client::ReadContext ctx(rsoplog, lockReason);
                Client::Transaction txn(DB_SERIALIZABLE);
                // if there is nothing in the oplog, break
                o = getLastEntryInOplog();
                if (o.isEmpty()) {
                    throw RollbackOplogException("Oplog empty when rolling back to a GTID");
                }
            }
            GTID lastGTID = getGTIDFromBSON("_id", o);
            // if we have rolled back enough, break from while loop
            if (GTID::cmp(lastGTID, idToRollbackTo) <= 0) {
                dassert(GTID::cmp(lastGTID, idToRollbackTo) == 0);
                break;
            }
            rollbackTransactionFromOplog(o, docsMap, rsSave);
        }
        log() << "Rolling back to " << idToRollbackTo.toString() << " produced " <<
            docsMap->size() << " documents for which we need to retrieve a snapshot of." << rsLog;
    }

    void removeDataFromDocsMap() {
        Client::Transaction txn(DB_SERIALIZABLE);
        RollbackDocsMapIterator docsMap;
        size_t numDocs = 0;
        log() << "Removing documents from collections for rollback." << rsLog;
        for (RollbackDocsMapIterator it; it.ok(); it.advance()){
            numDocs++;
            DocID curr = it.current();
            LOCK_REASON(lockReason, "repl: deleting a doc during rollback");
            Client::ReadContext ctx(curr.ns, lockReason);
            Collection* cl = getCollection(curr.ns);
            verify(cl);
            BSONObj currDoc;
            LOG(2) << "Finding by pk of " << curr.pk << rsLog;
            bool found = cl->findByPK(curr.pk, currDoc);
            if (found) {
                deleteOneObject(cl, curr.pk, currDoc, Collection::NO_LOCKTREE);
            }
        }
        log() << "Done removing " << numDocs << " documents from collections for rollback." << rsLog;
        updateRollbackStatus(BSON("_id" << ROLLBACK_ID << "state" << RB_DOCS_REMOVED<< \
            "info" << "removed docs from docs map"));
        txn.commit(DB_TXN_NOSYNC);
    }

    // on input, conn is a connection for which the caller has created a multi-statement
    // mvcc transaction over it. Reads the document from the remote server and
    // applies it locally
    void applySnapshotOfDocsMap(shared_ptr<DBClientConnection> conn) {
        size_t numDocs = 0;
        log() << "Applying documents to collections for rollback." << rsLog;
        for (RollbackDocsMapIterator it; it.ok(); it.advance()){
            numDocs++;
            DocID curr = it.current();
            LOCK_REASON(lockReason, "repl: appling snapshot of doc during rollback");
            Client::ReadContext ctx(curr.ns, lockReason);
            Collection* cl = getCollection(curr.ns);
            if (cl->isPKHidden()) {
                log() << "Collection " << curr.ns << " has a hidden PK, yet it has \
                    a document for which we want to apply a snapshot of: " << \
                    curr.pk << rsLog;
                throw RollbackOplogException("Collection for which we are applying a document has a hidden PK");
            }
            BSONObj pkWithFields = cl->fillPKWithFields(curr.pk);
            BSONObj remoteImage = findOneFromConn(conn.get(), curr.ns, Query(pkWithFields));
            if (!remoteImage.isEmpty()) {
                const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
                insertOneObject(cl, remoteImage, flags);
            }
        }
        log() << "Done applying remote images of " << numDocs << " documents to collections for rollback." << rsLog;
    }


    class RollbackGTIDSetBuilder {
        GTID _minUnappliedGTID;
        static void removeExistingGTIDSet() {
            string errmsg;
            Collection* cl = getCollection(rsRollbackGTIDSet);
            if (cl != NULL) {
                BSONObjBuilder result;
                cl->drop(errmsg,result);
                cl = NULL;
            }
        }
    public:
        RollbackGTIDSetBuilder(GTID minUnapplied) { 
            // write lock needed to (maybe) drop and recreate the
            // collection
            LOCK_REASON(lockReason, "repl: initializing set of committed GTIDs during rollback");
            Client::WriteContext ctx(rsRollbackGTIDSet, lockReason);
            removeExistingGTIDSet();            
            Collection* cl = getOrCreateCollection(rsRollbackGTIDSet, false);
            BSONObjBuilder docBuilder;
            docBuilder.append("_id", "minUnapplied");
            docBuilder.append("gtid", minUnapplied);
            const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
            BSONObj doc = docBuilder.done();
            insertOneObject(cl, doc, flags);                                
            _minUnappliedGTID = minUnapplied;
        }
        void addGTID(GTID gtid) {
            if (GTID::cmp(_minUnappliedGTID, gtid) <= 0) {
                Collection* cl = getCollection(rsRollbackGTIDSet);
                verify(cl);
                BSONObjBuilder docBuilder;
                docBuilder.append("_id", gtid);
                const uint64_t flags = Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE;
                BSONObj doc = docBuilder.obj();
                insertOneObject(cl, doc, flags);                                
            }
        }
        static void dropGTIDSet() {
            LOCK_REASON(lockReason, "repl: dropping set of committed GTIDs during rollback");
            Client::WriteContext ctx(rsRollbackGTIDSet, lockReason);
            removeExistingGTIDSet();
        }
    };

    class RollbackGTIDSet {
        GTID _minUnappliedGTID;
    public:
        RollbackGTIDSet() { }
        GTID getMinUnapplied() {
            return _minUnappliedGTID;
        }
        void initialize() {
            LOCK_REASON(lockReason, "repl rollback: initializing RollbackGTIDSet");
            Client::ReadContext ctx(rsRollbackGTIDSet, lockReason);
            Client::Transaction transaction(DB_READ_UNCOMMITTED);

            Collection* cl = getCollection(rsRollbackGTIDSet);
            verify(cl);
            BSONObj o;
            bool foundMinUnapplied = Collection::findOne(
                rsRollbackGTIDSet,
                BSON ("_id" << "minUnapplied"),
                o,
                true
                );
            verify(foundMinUnapplied);
            _minUnappliedGTID = getGTIDFromBSON("gtid", o);
            transaction.commit();
        }
        bool GTIDExists(GTID gtid) {
            if (GTID::cmp(gtid, _minUnappliedGTID) < 0) {
                return true;
            }
            LOCK_REASON(lockReason, "repl rollback: checking to see if a GTID exists");
            Client::ReadContext ctx(rsRollbackGTIDSet, lockReason);
            Client::Transaction transaction(DB_READ_UNCOMMITTED);
            BSONObj result;
            Collection* cl = getCollection(rsRollbackGTIDSet);
            verify(cl); // sanity check
            bool found = Collection::findOne(
                rsRollbackGTIDSet,
                BSON ("_id" << gtid),
                result,
                true
                );
            transaction.commit();
            return found;
        }
    };

    void createAppliedGTIDSet(
        GTID minUnapplied,
        shared_ptr<DBClientConnection> conn,
        RollbackGTIDSetBuilder* appliedGTIDs
        )
    {
        LOCK_REASON(lockReason, "repl: adding list of committed GTIDs during rollback");
        Client::ReadContext ctx(rsRollbackGTIDSet, lockReason);
        log() << "creating GTIDSet with minUnapplied " << minUnapplied.toString() << rsLog;
        uint64_t numGTIDs = 0;
        // we have gotten the minUnapplied and lastGTID on the remote server,
        // now let's look at the oplog from that point on to learn what the gaps are
        BSONObj query = BSON("_id" << BSON("$gte" << minUnapplied));
        const BSONObj fields = BSON("_id" << 1 << "a" << 1);
        shared_ptr<DBClientCursor> cursor(
            conn->query(
                rsoplog,
                Query(query).hint(BSON("_id" << 1)),
                0,
                0,
                &fields,
                QueryOption_SlaveOk
                ).release()
            );
        // now we have a cursor
        // put each doc we get back into a map
        uassert(17360, "Could not create a cursor to read remote oplog GTIDs during rollback", cursor.get());
        while (cursor->more()) {
            BSONObj curr = cursor->next();
            GTID gtid = getGTIDFromBSON("_id", curr);
            bool applied = curr["a"].Bool();
            if (applied) {
                appliedGTIDs->addGTID(gtid);
                numGTIDs++;
            }
        }
        log() << "done creating RollbackGTIDSet with " << numGTIDs << " GTIDs." << rsLog;
    }

    void applyInformationFromRemoteSnapshot(OplogReader& r) {
        shared_ptr<DBClientConnection> conn(r.conn_shared());
        RemoteTransaction rtxn(*conn, "mvcc");
        BSONObj lastOp = findOneFromConn(conn.get(), rsoplog, Query().sort(reverseIDObj));
        log() << "lastOp of snapshot " << lastOp << rsLog;
        GTID lastGTID = getGTIDFromBSON("_id", lastOp);
        uint64_t lastHash = lastOp["h"].numberLong();

        // two things to do, first is to get the snapshot of the oplog and find out what
        // gaps exist
        BSONObjBuilder b;
        b.append("_id", "minUnapplied");
        BSONObj res = findOneFromConn(conn.get(), rsReplInfo , b.done());
        GTID minUnapplied = getGTIDFromBSON("GTID", res);

        Client::Transaction txn(DB_SERIALIZABLE);
        RollbackGTIDSetBuilder appliedGTIDsBuilder(minUnapplied);
        createAppliedGTIDSet(minUnapplied, conn, &appliedGTIDsBuilder);
        // apply snapshot of each doc in docsMap
        applySnapshotOfDocsMap(conn);

        bool ok = rtxn.commit();
        verify(ok);  // absolutely no reason this should fail, it was read only

        RemoteTransaction rSecondTxn(*conn, "readUncommitted");
        BSONObj lastObjAfterSnapshotDone = r.getLastOp();
        log() << "lastOp after snapshot " << lastObjAfterSnapshotDone << rsLog;
        GTID lastGTIDAfterSnapshotDone = getGTIDFromBSON("_id", lastObjAfterSnapshotDone);
        uint64_t lastTSAfterSnapshotDone = lastObjAfterSnapshotDone["ts"]._numberLong();
        uint64_t lastHashAfterSnapshotDone = lastObjAfterSnapshotDone["h"].numberLong();
        ok = rSecondTxn.commit();
        verify(ok);  // absolutely no reason this should fail, it was read only

        BSONObjBuilder statusBuilder;
        statusBuilder.append("_id", ROLLBACK_ID);
        statusBuilder.append("state", RB_SNAPSHOT_APPLIED);
        statusBuilder.append("info", "applied snapshot data, about to run forward");
        statusBuilder.append("lastGTID", lastGTID);
        statusBuilder.append("lastHash", (long long)lastHash);
        statusBuilder.append("lastGTIDAfterSnapshotDone", lastGTIDAfterSnapshotDone);
        statusBuilder.appendDate("lastTSAfterSnapshotDone", lastTSAfterSnapshotDone);
        statusBuilder.append("lastHashAfterSnapshotDone", (long long)lastHashAfterSnapshotDone);
        updateRollbackStatus(statusBuilder.done());
        txn.commit(DB_TXN_NOSYNC);
    }

    void replicateForwardToGTID(
        OplogReader& r,
        bool preSnapshot
        )
    {
        // now replicate everything until we get to lastGTID
        // start from idToRollbackTo
        BSONObj o;
        bool found = getRollbackStatus(o);
        verify(found);
        log() << "Rollback status: " << o << rsLog;
        verify(o["state"].Int() == RB_SNAPSHOT_APPLIED);
        GTID lastGTID;
        uint64_t lastHash;
        if (preSnapshot) {
            lastGTID = getGTIDFromBSON("lastGTID", o);
            lastHash = o["lastHash"].numberLong();
        }
        else {
            lastGTID = getGTIDFromBSON("lastGTIDAfterSnapshotDone", o);
            lastHash = o["lastHashAfterSnapshotDone"].numberLong();
        }
        GTID startPoint;
        {
            Client::Transaction txn(DB_SERIALIZABLE);
            BSONObj start = getLastEntryInOplog();
            startPoint = getGTIDFromBSON("_id", start);
            txn.commit();
        }
        log () << "replicating from " << startPoint.toString() << " to " << lastGTID.toString() << rsLog;
        if (GTID::cmp(startPoint, lastGTID) >= 0) {
            log() << "returning early because startPoint >= lastGTID" << rsLog;
            return;
        }
        // let's first do a sanity check that the remote machine we are replicating from
        // does have the GTID and hash we intend to replicate to. If not, we have to go
        // fatal, as something went wrong.
        BSONObj targetObj = r.findOpWithGTID(lastGTID);
        if (targetObj.isEmpty() || ((uint64_t)targetObj["h"].numberLong()) != lastHash) {
            if (targetObj.isEmpty()) {
                log() << "target did not have the GTID we were looking for" << targetObj << rsLog;
            }
            else {
                log() << "our hash " << lastHash << "their hash " << targetObj["h"].numberLong() << rsLog;
            }
            throw RollbackOplogException("Member we are syncing from does not have the target GTID we wish to replicate to.");
        }
        
        r.resetCursor();
        r.tailingQueryGTE(rsoplog, startPoint);
        uassert(17361, "Cursor died while running replicateForwardToGTID", r.haveCursor());

        // appliedGTIDs only used if preSnapshot is true
        RollbackGTIDSet appliedGTIDs;
        appliedGTIDs.initialize();
        BSONObj checkFirstObj = r.next().getOwned();
        GTID currGTID = getGTIDFromBSON("_id", checkFirstObj);
        if (GTID::cmp(startPoint, currGTID) != 0) {
            throw RollbackOplogException("Could not find startPoint on remote server");
        }
        while (GTID::cmp(currGTID, lastGTID) < 0) {
            while (r.more()) {
                BSONObj curr = r.nextSafe().getOwned();
                currGTID = getGTIDFromBSON("_id", curr);
                if (GTID::cmp(currGTID, lastGTID) > 0) {
                    break; // we are done
                }
                {
                    bool bigTxn = false;
                    Client::Transaction transaction(DB_SERIALIZABLE);
                    replicateFullTransactionToOplog(curr, r, &bigTxn);
                    // we are operating as a secondary. We don't have to fsync
                    transaction.commit(DB_TXN_NOSYNC);
                }
                // apply the transaction if it existed in snapshot
                bool applyTransaction = true;
                if (preSnapshot) {
                    applyTransaction = appliedGTIDs.GTIDExists(currGTID);
                }
                if (applyTransaction) {
                    RollbackDocsMap docsMap;
                    applyTransactionFromOplog(curr, preSnapshot ? &docsMap : NULL, true);
                }
                if (GTID::cmp(currGTID, lastGTID) == 0) {
                    break; // we are done
                }
            }
            r.tailCheck();
            uassert(17362, "Cursor died while running replicateForwardToGTID", r.haveCursor());
        }
        log () << "Done replicating from " << startPoint.toString() << " to " << lastGTID.toString() << rsLog;
    }

    void finishRollback(bool finishedEarly) {
        BSONObj o;
        bool found = getRollbackStatus(o);
        verify(found);
        if (!finishedEarly) {
            verify(o["state"].Int() == RB_SNAPSHOT_APPLIED);
            GTID lastGTIDAfterSnapshotDone = getGTIDFromBSON("lastGTIDAfterSnapshotDone", o);
            uint64_t lastTS = o["lastTSAfterSnapshotDone"]._numberLong();
            uint64_t lastHash = o["lastHashAfterSnapshotDone"].numberLong();
            theReplSet->gtidManager->resetAfterInitialSync(
                lastGTIDAfterSnapshotDone,
                lastTS,
                lastHash
                );
            // now force an update of the repl info thread
            theReplSet->forceUpdateReplInfo();
        }
        // now delete a bunch of stuff
        Client::Transaction txn(DB_SERIALIZABLE);
        // remove docsMap and RollbackGTIDSet
        RollbackGTIDSetBuilder::dropGTIDSet();
        RollbackDocsMap::dropDocsMap();
        clearRollbackStatus(o);
        txn.commit(DB_TXN_NOSYNC);
        theReplSet->leaveRollbackState();
    }

    uint32_t runRollbackInternal(OplogReader& r, uint64_t oplogTS) {
        enum rollback_state startState = RB_NOT_STARTED;
        BSONObj o;
        if (getRollbackStatus(o)) {
            startState = static_cast<enum rollback_state>(o["state"].Int());
            verify(startState > RB_NOT_STARTED && startState <= RB_SNAPSHOT_APPLIED);
        }
        if (startState == RB_STARTING) {
            log() << "Rollback found in RB_STARTING phase, cannot recover" << rsLog;
            throw RollbackOplogException("Rollback was left in state we cannot recover from");
        }

        if (startState == RB_NOT_STARTED) {
            // starting from ourLast, we need to read the remote oplog
            // backwards until we find an entry in the remote oplog
            // that has the same GTID, timestamp, and hash as
            // what we have in our oplog. If we don't find one that is within
            // some reasonable timeframe, then we go fatal
            GTID idToRollbackTo;
            uint64_t rollbackPointTS = 0;
            uint64_t rollbackPointHash = 0;

            findRollbackPoint(
                r,
                oplogTS,
                &idToRollbackTo,
                &rollbackPointTS,
                &rollbackPointHash
                );

            if (!canStartRollback(r, idToRollbackTo)) {
                return 2;
            }
            startRollback(idToRollbackTo, rollbackPointTS, rollbackPointHash);
            RollbackDocsMap docsMap; // stores the documents we need to get images of
            docsMap.initialize();
            RollbackSaveData rsSave;
            rsSave.initialize();
            rollbackToGTID(idToRollbackTo, &docsMap, &rsSave);
            if (docsMap.size() == 0) {
                log() << "Rollback produced no docs in docsMap, exiting early" << rsLog;
                finishRollback(true);
                theReplSet->leaveRollbackState();
                return 0;
            }
            // at this point docsMap has the list of documents (identified by collection and pk)
            // that we need to get a snapshot of
            removeDataFromDocsMap();
        }

        if (startState < RB_SNAPSHOT_APPLIED) {
            applyInformationFromRemoteSnapshot(r);
        }

        replicateForwardToGTID(r, true);        
        log() << "Applying missing ops for rollback" << rsLog;
        RollbackGTIDSet appliedGTIDs;
        appliedGTIDs.initialize();
        applyMissingOpsInOplog(appliedGTIDs.getMinUnapplied(), true);
        log() << "Done applying missing ops for rollback" << rsLog;
        
        // while we were applying the remote images above, a collection may have been dropped
        // That drop will be associated with a point in time after lastGTID. For that reason,
        // we cannot transition to secondary just yet, because there may be data missing
        // from the point in time associated with lastGTID. Therefore, to be safe,
        // we play forward to lastGTIDAfterSnapshotDone, a point in time taken after
        // we committed our MVCC transaction
        replicateForwardToGTID(r, false);
        finishRollback(false);
        return 0;
    }

    void BackgroundSync::settleApplierForRollback() {
        boost::unique_lock<boost::mutex> lock(_mutex);
        while (_deque.size() > 0) {
            log() << "waiting for applier to finish work before doing rollback " << rsLog;
            _queueDone.wait(lock);
        }
        verifySettled();
    }

    uint32_t BackgroundSync::runRollback(OplogReader& r, uint64_t oplogTS) {
        log() << "Starting to run rollback" << rsLog;
        // first, let's get all the operations that are being applied out of the way,
        // we don't want to rollback an item in the oplog while simultaneously,
        // the applier thread is applying it to the oplog
        settleApplierForRollback();
        {
            // so we know nothing is simultaneously occurring
            RWLockRecursive::Exclusive e(operationLock);
            LOCK_REASON(lockReason, "repl: killing all operations for rollback");
            Lock::GlobalWrite lk(lockReason);
            ClientCursor::invalidateAllCursors();
            Client::abortLiveTransactions();
            theReplSet->goToRollbackState();
            // we leave rollback state upon successful
            // execution of runRollbackInternal, which we call below
        }
        for (uint32_t attempts = 0; attempts < 10; attempts++) {
            try {
                if (attempts == 0) {
                    return runRollbackInternal(r, oplogTS);
                }
                else {
                    OplogReader newReader (false);
                    // find a target to sync from the last op time written
                    getOplogReader(&newReader);
                    // no server found
                    bool gotReader = true;
                    {
                        boost::unique_lock<boost::mutex> lock(_mutex);                    
                        if (_currentSyncTarget == NULL) {
                            gotReader = false;
                        }
                    }
                    if (gotReader) {
                        return runRollbackInternal(newReader, oplogTS);
                    }
                }
            }
            catch (RollbackOplogException& e) {
                throw;
            }
            catch (DBException& e) {
                log() << "Caught DBException during rollback " << e.toString() << rsLog;
            }
            catch (std::exception& e2) {
                log() << "Caught std::exception during rollback " << e2.what() << rsLog;
            }
            sleepsecs(1);
        }
        // after trying repeatedly, we eventually give up
        throw RollbackOplogException("Attempt to rollback has failed.");
    }
} // namespace mongo
