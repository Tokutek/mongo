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

#include "mongo/client/remote_transaction.h"
#include "mongo/db/client.h"
#include "mongo/db/cursor.h"
#include "mongo/db/instance.h"
#include "mongo/db/jsobjmanipulator.h"
#include "mongo/db/oplog.h"
#include "mongo/db/oplogreader.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/repl.h"
#include "mongo/db/repl/bgsync.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/repl/rs_optime.h"
#include "mongo/db/repl/rs_sync.h"
#include "mongo/db/storage/env.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    using namespace mongoutils;
    using namespace bson;

    static void dropAllDatabasesExceptLocal() {
        vector<string> n;
        getDatabaseNames(n);
        LOG(0) << "dropAllDatabasesExceptLocal " << n.size() << endl;
        for (vector<string>::const_iterator i = n.begin(); i != n.end(); i++) {
            if (*i != "local") {
                Client::Context ctx(*i);
                dropDatabase(*i);
            }
        }
    }

    // add try/catch with sleep

    void isyncassert(const string& msg, bool expr) {
        if( !expr ) {
            string m = str::stream() << "initial sync " << msg;
            theReplSet->sethbmsg(m, 0);
            uasserted(13404, m);
        }
    }

    void ReplSetImpl::syncDoInitialSync() {
        static const int maxFailedAttempts = 10;
        int failedAttempts = 0;
        bool syncSucceeded = false;
        while ( !syncSucceeded && failedAttempts < maxFailedAttempts ) {
            try {
                syncSucceeded = _syncDoInitialSync();
            }
            catch(DBException& e) {
                failedAttempts++;
                str::stream msg;
                msg << "initial sync exception: ";
                msg << e.toString() << " " << (maxFailedAttempts - failedAttempts) << " attempts remaining" ;
                sethbmsg(msg, 0);
                sleepsecs(30);
            }
        }
        fassert( 16233, failedAttempts < maxFailedAttempts);
    }

    /* todo : progress metering to sethbmsg. */
    static bool clone(
        const char *master, 
        const std::string& db,
        shared_ptr<DBClientConnection> conn,
        bool syncIndexes
        ) 
    {
        CloneOptions options;

        options.fromDB = db;

        options.logForRepl = false;
        options.slaveOk = true;
        options.useReplAuth = true;
        options.mayBeInterrupted = false;
        
        options.syncData = true;
        options.syncIndexes = syncIndexes;

        string err;
        return cloneFrom(master, options, conn, err);
    }


    bool ReplSetImpl::_syncDoInitialSync_clone( 
        const char *master, 
        const list<string>& dbs,
        shared_ptr<DBClientConnection> conn
        ) 
    {
        verify(Lock::isW());
        for (list<string>::const_iterator i = dbs.begin(); i != dbs.end(); i++) {
            string db = *i;
            if (db == "local") {
                continue;
            }
            
            sethbmsg(str::stream() << "initial sync cloning db: " << db, 0);

            Client::Context ctx(db);
            if (!clone(master, db, conn, _buildIndexes)) {
                sethbmsg(str::stream() << "initial sync error clone of " << db << " failed sleeping 5 minutes", 0);
                return false;
            }
        }

        return true;
    }

    bool Member::syncable() const {
        bool buildIndexes = theReplSet ? theReplSet->buildIndexes() : true;
        return hbinfo().up() && (config().buildIndexes || !buildIndexes) && state().readable();
    }

    Member* ReplSetImpl::getMemberToSyncTo() {
        lock lk(this);
        GTID lastGTID = gtidManager->getLiveState();

        // if we have a target we've requested to sync from, use it

        if (_forceSyncTarget) {
            Member* target = _forceSyncTarget;
            _forceSyncTarget = 0;
            sethbmsg( str::stream() << "syncing to: " << target->fullName() << " by request", 0);
            return target;
        }

        Member* primary = const_cast<Member*>(box.getPrimary());

        // wait for 2N pings before choosing a sync target
        if (_cfg) {
            int needMorePings = config().members.size()*2 - HeartbeatInfo::numPings;

            if (needMorePings > 0) {
                OCCASIONALLY log() << "waiting for " << needMorePings << " pings from other members before syncing" << endl;
                return NULL;
            }

            // If we are only allowed to sync from the primary, return that
            if (!_cfg->chainingAllowed()) {
                // Returns NULL if we cannot reach the primary
                return primary;
            }
        }

        // find the member with the lowest ping time that has more data than me

        // Find primary's oplog time. Reject sync candidates that are more than
        // maxSyncSourceLagSecs seconds behind.
        uint64_t primaryOpTime;
        uint64_t maxSyncSourceLagMillis = maxSyncSourceLagSecs*1000;
        if (primary) {
            primaryOpTime = primary->hbinfo().opTime;
        }
        else {
            // choose a time that will exclude no candidates, since we don't see a primary
            primaryOpTime = maxSyncSourceLagMillis;
        }
        
        if ( primaryOpTime < maxSyncSourceLagMillis ) {
            // erh - I think this means there was just a new election
            // and we don't yet know the new primary's optime
            primaryOpTime = maxSyncSourceLagMillis;
        }

        uint64_t oldestSyncOpTime = primaryOpTime - maxSyncSourceLagMillis;

        Member *closest = 0;
        time_t now = 0;

        // Make two attempts.  The first attempt, we ignore those nodes with
        // slave delay higher than our own.  The second attempt includes such
        // nodes, in case those are the only ones we can reach.
        // This loop attempts to set 'closest'.
        for (int attempts = 0; attempts < 2; ++attempts) {
            for (Member *m = _members.head(); m; m = m->next()) {
                if (!m->syncable()) {
                     continue;
                }

                if (m->state() == MemberState::RS_SECONDARY) {
                    // only consider secondaries that are ahead of where we are
                    if (GTID::cmp(m->hbinfo().gtid, lastGTID) <= 0) {
                        continue;
                    }
                    // omit secondaries that are excessively behind, on the first attempt at least.
                    if (attempts == 0 &&
                        m->hbinfo().opTime < oldestSyncOpTime) 
                    {
                        continue;
                    }
                }

                // omit nodes that are more latent than anything we've already considered
                if (closest &&
                    (m->hbinfo().ping > closest->hbinfo().ping))
                    continue;

                if (attempts == 0 &&
                    (myConfig().slaveDelay < m->config().slaveDelay || m->config().hidden)) {
                    continue; // skip this one in the first attempt
                }

                map<string,time_t>::iterator vetoed = _veto.find(m->fullName());
                if (vetoed != _veto.end()) {
                    // Do some veto housekeeping
                    if (now == 0) {
                        now = time(0);
                    }

                    // if this was on the veto list, check if it was vetoed in the last "while".
                    // if it was, skip.
                    if (vetoed->second >= now) {
                        if (time(0) % 5 == 0) {
                            log() << "replSet not trying to sync from " << (*vetoed).first
                                  << ", it is vetoed for " << ((*vetoed).second - now) << " more seconds" << rsLog;
                        }
                        continue;
                    }
                    _veto.erase(vetoed);
                    // fall through, this is a valid candidate now
                }
                // This candidate has passed all tests; set 'closest'
                closest = m;
            }
            if (closest) break; // no need for second attempt
        }

        if (!closest) {
            return NULL;
        }

        sethbmsg( str::stream() << "syncing to: " << closest->fullName(), 0);

        return closest;
    }

    void ReplSetImpl::veto(const string& host, const unsigned secs) {
        lock lk(this);
        _veto[host] = time(0)+secs;
    }

    void ReplSetImpl::_fillGaps(OplogReader* r) {
        Client::ReadContext ctx(rsoplog);
        Client::Transaction catchupTransaction(0);
        NamespaceDetails *d = nsdetails(rsReplInfo);
        
        // now we should have replInfo on this machine,
        // let's query the minLiveGTID to figure out from where
        // we should copy the opLog
        BSONObj result;
        const bool foundMinLive = d != NULL &&
            d->findOne(
               BSON( "_id" << "minLive" ),
               result
               );
        verify(foundMinLive);
        GTID minLiveGTID;
        minLiveGTID = getGTIDFromBSON("GTID", result);
        // now we need to read the oplog forward
        GTID lastEntry;
        bool ret = getLastGTIDinOplog(&lastEntry);
        isyncassert("could not get last oplog entry after clone", ret);
        
        GTID currEntry = minLiveGTID;
        // first, we need to fill in the "gaps" in the oplog
        while (GTID::cmp(currEntry, lastEntry) < 0) {
            r->tailingQueryGTE(rsoplog, currEntry);
            while (GTID::cmp(currEntry, lastEntry) < 0) {
                bool hasMore = true;
                if (!r->moreInCurrentBatch()) {
                    hasMore = r->more();
                }
                if (!hasMore) {
                    break;
                }
                BSONObj op = r->nextSafe().getOwned();
                currEntry = getGTIDFromOplogEntry(op);
                // try inserting it into the oplog, if it does not
                // already exist
                if (!gtidExistsInOplog(currEntry)) {
                    bool bigTxn;
                    replicateFullTransactionToOplog(op, *r, &bigTxn);
                }
            }
        }
        catchupTransaction.commit(0);
    }

    void ReplSetImpl::_applyMissingOpsDuringInitialSync() {
        std::deque<BSONObj> unappliedTransactions;
        {
            // accumulate a list of transactions that are unapplied
            Client::ReadContext ctx(rsoplog);
            Client::Transaction catchupTransaction(0);        
            NamespaceDetails *d = nsdetails(rsReplInfo);

            // now we should have replInfo on this machine,
            // let's query the minUnappliedGTID to figure out from where
            // we should copy the opLog
            BSONObj result;
            const bool foundMinUnapplied = d != NULL &&
                d->findOne(
                   BSON( "_id" << "minUnapplied" ), 
                   result
                   );
            verify(foundMinUnapplied);
            GTID minUnappliedGTID;
            minUnappliedGTID = getGTIDFromBSON("GTID", result);
            // now we need to read the oplog forward
            GTID lastEntry;
            bool ret = getLastGTIDinOplog(&lastEntry);
            isyncassert("could not get last oplog entry after clone", ret);
            
            // at this point, we have got the oplog up to date,
            // now we need to read forward in the oplog
            // from minUnapplied
            BSONObjBuilder q;
            addGTIDToBSON("$gte", minUnappliedGTID, q);
            BSONObjBuilder query;
            query.append("_id", q.done());
            
            {
                shared_ptr<Cursor> c = getOptimizedCursor(rsoplog, query.done());
                while( c->ok() ) {
                    if ( c->currentMatches()) {
                        BSONObj curr = c->current();                    
                        bool transactionAlreadyApplied = curr["a"].Bool();
                        if (!transactionAlreadyApplied) {
                            unappliedTransactions.push_back(curr.getOwned());
                        }
                    }
                    c->advance();
                }
            }
            catchupTransaction.commit(0);
        }
        while (unappliedTransactions.size() > 0) {
            BSONObj curr = unappliedTransactions.front();
            applyTransactionFromOplog(curr);            
            unappliedTransactions.pop_front();
        }
    }
    
    /**
     * Do the initial sync for this member.
     * This code can use a little refactoring. bit ugly
     */
    bool ReplSetImpl::_syncDoInitialSync() {
        sethbmsg("initial sync pending",0);
        bool needsFullSync = gtidManager->getLiveState().isInitial();
        bool needGapsFilled = needsFullSync || replSettings.fastsync;

        // if this is the first node, it may have already become primary
        if ( box.getState().primary() ) {
            sethbmsg("I'm already primary, no need for initial sync",0);
            return true;
        }

        const Member *source = NULL;
        OplogReader r;
        string sourceHostname;
        // only bother making a connection if we need to connect for some reason
        if (needGapsFilled) {
            source = getMemberToSyncTo();
            if (!source) {
                sethbmsg("initial sync need a member to be primary or secondary to do our initial sync", 0);
                sleepsecs(15);
                return false;
            }

            sourceHostname = source->h().toString();
            if( !r.connect(sourceHostname) ) {
                sethbmsg( str::stream() << "initial sync couldn't connect to " << source->h().toString() , 0);
                sleepsecs(15);
                return false;
            }
        }

        if( needsFullSync ) {
            BSONObj lastOp = r.getLastOp(rsoplog);
            if( lastOp.isEmpty() ) {
                sethbmsg("initial sync couldn't read remote oplog", 0);
                sleepsecs(15);
                return false;
            }

            {
                Lock::GlobalWrite lk;
                Client::Transaction dropTransaction(DB_SERIALIZABLE);
                sethbmsg("initial sync drop all databases", 0);
                dropAllDatabasesExceptLocal();
                dropTransaction.commit();
            }

            // now deal with creation of oplog
            // first delete any existing data in the oplog

            {
                Lock::DBWrite lk("local");
                Client::Transaction fileOpsTransaction(DB_SERIALIZABLE);
                deleteOplogFiles();
                // now recreate the oplog
                createOplog();
                openOplogFiles();
                fileOpsTransaction.commit(0);
            }

            try {
                sethbmsg("initial sync clone all databases", 0);
            
                shared_ptr<DBClientConnection> conn(r.conn_shared());
                RemoteTransaction rtxn(*conn, "mvcc");

                list<string> dbs = conn->getDatabaseNamesForRepl();

                //
                // Not sure if it is necessary to have a separate fileOps 
                // transaction and clone transaction. The cloneTransaction
                // has a higher chance of failing, and I don't know at the moment
                // if it is ok to do fileops successfully, and then an operation (cloning) that
                // later causes an abort. So, to be cautious, they are separate

                {
                    Lock::GlobalWrite lk;
                    Client::Transaction cloneTransaction(DB_SERIALIZABLE);
                    bool ret = _syncDoInitialSync_clone(sourceHostname.c_str(), dbs, conn);

                    if (!ret) {
                        veto(source->fullName(), 600);
                        sleepsecs(300);
                        return false;
                    }

                    // at this point, we have copied all of the data from the 
                    // remote machine. Now we need to copy the replication information
                    // on the remote machine's local database, we need to copy
                    // the entire (small) replInfo dictionary, and the necessary portion
                    // of the oplog

                    // first copy the replInfo, as we will use its information
                    // to determine  how much of the opLog to copy
                    BSONObj q;
                    cloneCollectionData(conn,
                                        rsReplInfo,
                                        q,
                                        true, //copyIndexes
                                        false //logForRepl
                                        );

                    // copy entire oplog (probably overkill)
                    cloneCollectionData(conn,
                                        rsoplog,
                                        q,
                                        true, //copyIndexes
                                        false //logForRepl
                                        );

                    // copy entire oplog.refs (probably overkill)
                    cloneCollectionData(conn,
                                        rsOplogRefs,
                                        q,
                                        true, //copyIndexes
                                        false //logForRepl
                                        );
                    cloneTransaction.commit(0);
                }

                bool ok = rtxn.commit();
                verify(ok);  // absolutely no reason this should fail, it was read only
                // data should now be consistent
            }
            catch (DBException &e) {
                sethbmsg("exception trying to copy data", 0);
                LOG(0) << e.getCode() << ": " << e.what() << endl;
                sleepsecs(1);
                return false;
            }
        }
        else {
            Lock::DBWrite lk("local");
            openOplogFiles();
        }
        if (needGapsFilled) {
            _fillGaps(&r);
        }
        _applyMissingOpsDuringInitialSync();

        sethbmsg("initial sync done",0);

        return true;
    }
}
