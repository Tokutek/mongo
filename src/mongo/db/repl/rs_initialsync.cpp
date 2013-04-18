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

#include "mongo/db/repl/rs.h"

#include "mongo/db/client.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/oplog.h"
#include "mongo/db/oplogreader.h"
#include "mongo/db/repl.h"
#include "mongo/db/repl/bgsync.h"
#include "mongo/db/repl/rs_optime.h"
#include "mongo/db/repl/rs_sync.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/db/instance.h"
#include "mongo/db/jsobjmanipulator.h"

namespace mongo {

    using namespace mongoutils;
    using namespace bson;

    static void dropAllDatabasesExceptLocal() {
        Lock::GlobalWrite lk;

        vector<string> n;
        getDatabaseNames(n);
        if( n.size() == 0 ) return;
        log() << "dropAllDatabasesExceptLocal " << n.size() << endl;
        for( vector<string>::iterator i = n.begin(); i != n.end(); i++ ) {
            if( *i != "local" ) {
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
        const static int maxFailedAttempts = 10;
        int failedAttempts = 0;
        while ( failedAttempts < maxFailedAttempts ) {
            try {
                _syncDoInitialSync();
                break;
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
        string db,
        shared_ptr<DBClientConnection> conn
        ) 
    {
        CloneOptions options;

        options.fromDB = db;

        options.logForRepl = false;
        options.slaveOk = true;
        options.useReplAuth = true;
        options.mayYield = true;
        options.mayBeInterrupted = false;
        
        options.syncData = true;
        options.syncIndexes = true;

        string err;
        return cloneFrom(master, options, conn, err);
    }


    bool ReplSetImpl::_syncDoInitialSync_clone( 
        const char *master, 
        const list<string>& dbs,
        shared_ptr<DBClientConnection> conn
        ) 
    {
        for( list<string>::const_iterator i = dbs.begin(); i != dbs.end(); i++ ) {
            string db = *i;
            if( db == "local" ) 
                continue;
            
            sethbmsg( str::stream() << "initial sync cloning db: " << db , 0);

            Client::WriteContext ctx(db);
            if ( ! clone( master, db,  conn) ) {
                sethbmsg( str::stream() 
                              << "initial sync error clone of " << db 
                              << " failed sleeping 5 minutes" ,0);
                return false;
            }
        }

        return true;
    }

    Member* ReplSetImpl::getMemberToSyncTo() {
        lock lk(this);
        GTID lastGTID = gtidManager->getLiveState();

        bool buildIndexes = true;

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

            buildIndexes = myConfig().buildIndexes;

            // If we are only allowed to sync from the primary, return that
            if (!_cfg->chainingAllowed()) {
                // Returns NULL if we cannot reach the primary
                return primary;
            }
        }

        // find the member with the lowest ping time that has more data than me

        // Find primary's oplog time. Reject sync candidates that are more than
        // MAX_SLACK_TIME seconds behind.
        uint64_t primaryOpTime;
        static const unsigned maxSlackDurationMillis = 10 * 60 * 1000; // 10 minutes
        if (primary) {
            primaryOpTime = primary->hbinfo().opTime;
        }
        else {
            // choose a time that will exclude no candidates, since we don't see a primary
            primaryOpTime = maxSlackDurationMillis;
        }
        
        if ( primaryOpTime < maxSlackDurationMillis ) {
            // erh - I think this means there was just a new election
            // and we don't yet know the new primary's optime
            primaryOpTime = maxSlackDurationMillis;
        }

        uint64_t oldestSyncOpTime = primaryOpTime - maxSlackDurationMillis;

        Member *closest = 0;
        time_t now = 0;

        // Make two attempts.  The first attempt, we ignore those nodes with
        // slave delay higher than our own.  The second attempt includes such
        // nodes, in case those are the only ones we can reach.
        // This loop attempts to set 'closest'.
        for (int attempts = 0; attempts < 2; ++attempts) {
            for (Member *m = _members.head(); m; m = m->next()) {
                if (!m->hbinfo().up())
                    continue;
                // make sure members with buildIndexes sync from other members w/indexes
                if (buildIndexes && !m->config().buildIndexes)
                    continue;

                if (!m->state().readable())
                    continue;

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

    /**
     * Do the initial sync for this member.
     */
    void ReplSetImpl::_syncDoInitialSync() {
        sethbmsg("initial sync pending",0);

        // if this is the first node, it may have already become primary
        if ( box.getState().primary() ) {
            sethbmsg("I'm already primary, no need for initial sync",0);
            return;
        }

        const Member *source = getMemberToSyncTo();
        if (!source) {
            sethbmsg("initial sync need a member to be primary or secondary to do our initial sync", 0);
            sleepsecs(15);
            return;
        }

        string sourceHostname = source->h().toString();
        OplogReader r;
        if( !r.connect(sourceHostname) ) {
            sethbmsg( str::stream() << "initial sync couldn't connect to " << source->h().toString() , 0);
            sleepsecs(15);
            return;
        }

        BSONObj lastOp = r.getLastOp(rsoplog);
        if( lastOp.isEmpty() ) {
            sethbmsg("initial sync couldn't read remote oplog", 0);
            sleepsecs(15);
            return;
        }

        GTID minUnappliedGTID;
        if (replSettings.fastsync) {
            log() << "fastsync: skipping database clone" << rsLog;

            // prime oplog
            //init.oplogApplication(lastOp, lastOp);
            ::abort();
            return;
        }
        else {
            sethbmsg("initial sync drop all databases", 0);
            dropAllDatabasesExceptLocal();

            // now deal with creation of oplog
            // first delete any existing data in the oplog
            Client::Transaction fileOpsTransaction(DB_SERIALIZABLE);
            deleteOplogFiles();
            // now recreate the oplog
            createOplog();
            openOplogFiles();
            fileOpsTransaction.commit(0);

            ::abort();
            sethbmsg("initial sync clone all databases", 0);

            
            BSONObj beginCommand = BSON( 
                "beginTransaction" << 1 << 
                "isolation" << "mvcc"
                );
            BSONObj commandRet;
            if( !r.conn()->runCommand("local", beginCommand, commandRet)) {
                sethbmsg("failed to begin transaction for copying data", 0);
                sleepsecs(1);
                return;
            }

            list<string> dbs = r.conn()->getDatabaseNames();

            //
            // Not sure if it is necessary to have a separate fileOps 
            // transaction and clone transaction. The cloneTransaction
            // has a higher chance of failing, and I don't know at the moment
            // if it is ok to do fileops successfully, and then an operation (cloning) that
            // later causes an abort. So, to be cautious, they are separate
            Client::Transaction cloneTransaction(DB_SERIALIZABLE);
            bool ret = _syncDoInitialSync_clone(
                sourceHostname.c_str(), 
                dbs, 
                r.conn_shared()
                );

            if (!ret) {
                veto(source->fullName(), 600);
                sleepsecs(300);
                return;
            }

            // at this point, we have copied all of the data from the 
            // remote machine. Now we need to copy the replication information
            // on the remote machine's local database, we need to copy
            // the entire (small) replInfo dictionary, and the necessary portion
            // of the oplog
            {                
                Client::WriteContext ctx(rsReplInfo);
                // first copy the replInfo, as we will use its information
                // to determine  how much of the opLog to copy
                BSONObj q;
                cloneCollectionData(
                    r.conn_shared(),
                    rsReplInfo,
                    q,
                    true, //copyIndexes
                    false //logForRepl
                    );

                // now we should have replInfo on this machine,
                // let's query the minUnappliedGTID to figure out from where
                // we should copy the opLog
                BSONObj result;
                bool foundMinUnapplied = Helpers::findOne(
                    rsReplInfo, 
                    BSON( "_id" << "minUnapplied" ), 
                    result
                    );
                // just for debugging for now
                if (foundMinUnapplied) {
                    minUnappliedGTID = getGTIDFromBSON("GTID", result);
                    log() << "foundMinUnapplied" << rsLog;
                    // copy the oplog with a query
                    int len;
                    cloneCollectionData(
                        r.conn_shared(),
                        rsoplog,
                        BSON( "_id" << GTE << result["GTID"].binData(len) ),
                        true, //copyIndexes
                        false //logForRepl
                        );
                }
                else {
                    log() << "did not find min unapplied" << rsLog;
                    // copy entire oplog
                    cloneCollectionData(
                        r.conn_shared(),
                        rsoplog,
                        q,
                        true, //copyIndexes
                        false //logForRepl
                        );
                }
            }
            cloneTransaction.commit(0);

            BSONObj commitCommand = BSON("commitTransaction" << 1);
            if (!r.conn()->runCommand("local", commitCommand, commandRet)) {
                sethbmsg("failed to commit transaction for copying data", 0);
                sleepsecs(1);
                return;
            }
            // data should now be consistent

        }

        // now we need to read the oplog forward
        GTID lastEntry;
        bool ret = getLastGTIDinOplog(&lastEntry);
        isyncassert("could not get last oplog entry after clone", ret);

        // TODO: query minLive and use that instead
        GTID currEntry = minUnappliedGTID;
        // first, we need to fill in the "gaps" in the oplog
        while (GTID::cmp(currEntry, lastEntry) < 0) {
            r.tailingQueryGTE(rsoplog, currEntry);
            while (GTID::cmp(currEntry, lastEntry) < 0) {
                bool hasMore = true;
                if (!r.moreInCurrentBatch()) {
                    hasMore = r.more();
                }
                if (!hasMore) {
                    break;
                }
                BSONObj op = r.nextSafe().getOwned();
                currEntry = getGTIDFromOplogEntry(op);
                replicateTransactionToOplog(op);
            }
        }

        // at this point, we have got the oplog up to date,
        // now we need to read forward in the oplog
        // from minUnapplied
        BSONObjBuilder query;
        addGTIDToBSON("$gte", minUnappliedGTID, query);
        // TODO: make this a read uncommitted cursor
        // especially when this code moves to a background thread
        // for running replication
        shared_ptr<Cursor> c = NamespaceDetailsTransient::getCursor(rsoplog, query.done());
        while( c->ok() ) {
            if ( c->currentMatches()) {
                applyTransactionFromOplog(c->current());
            }
            c->advance();
        }

        // TODO: figure out what to do with these
        verify( !box.getState().primary() ); // wouldn't make sense if we were.

        changeState(MemberState::RS_RECOVERING);
        sethbmsg("initial sync done",0);
    }
}
