// d_migrate.cpp

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


/**
   these are commands that live in mongod
   mostly around shard management and checking
 */

#include <algorithm>
#include <boost/thread/thread.hpp>
#include <map>
#include <string>
#include <vector>

#include <boost/thread/thread.hpp>

#include "mongo/pch.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/crash.h"
#include "mongo/db/database.h"
#include "mongo/db/commands.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/hasher.h"
#include "mongo/db/cmdline.h"
#include "mongo/db/query_optimizer_internal.h"
#include "mongo/db/cursor.h"
#include "mongo/db/repl_block.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/instance.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/repl.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/kill_current_op.h"
#include "mongo/db/ops/query.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/txn_context.h"
#include "mongo/db/collection.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/relock.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/storage/assert_ids.h"

#include "mongo/client/connpool.h"
#include "mongo/client/distlock.h"
#include "mongo/client/dbclientcursor.h"
#include "mongo/client/remote_transaction.h"

#include "mongo/util/queue.h"
#include "mongo/util/startup_test.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/ramlog.h"
#include "mongo/util/elapsed_tracker.h"

#include "mongo/s/chunk.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/config.h"
#include "mongo/s/d_logic.h"
#include "mongo/s/shard.h"
#include "mongo/s/type_chunk.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/elapsed_tracker.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/queue.h"
#include "mongo/util/ramlog.h"
#include "mongo/util/startup_test.h"

using namespace std;

namespace mongo {

    MONGO_EXPORT_SERVER_PARAMETER(migrateUniqueChecks, bool, true);
    MONGO_EXPORT_SERVER_PARAMETER(migrateStartCloneLockTimeout, uint64_t, 60000);

    bool findShardKeyIndexPattern_locked( const string& ns,
                                          const BSONObj& shardKeyPattern,
                                          BSONObj* indexPattern ) {
        verify( Lock::isLocked() );
        Collection *cl = getCollection( ns );
        if ( !cl )
            return false;
        const IndexDetails* idx = cl->findIndexByPrefix( shardKeyPattern , true );  /* require single key */
        if ( !idx )
            return false;
        *indexPattern = idx->keyPattern().getOwned();
        return true;
    }

    Tee* migrateLog = new RamLog( "migrate" );

    class MoveTimingHelper {
    public:
        MoveTimingHelper( const string& where , const string& ns , BSONObj min , BSONObj max , int total , string& cmdErrmsg )
            : _where( where ) , _ns( ns ) , _next( 0 ) , _total( total ) , _cmdErrmsg( cmdErrmsg ) {
            _nextNote = 0;
            _b.append( "min" , min );
            _b.append( "max" , max );
        }

        ~MoveTimingHelper() {
            // even if logChange doesn't throw, bson does
            // sigh
            try {
                if ( _next != _total ) {
                    note( "aborted" );
                }
                if ( _cmdErrmsg.size() ) {
                    note( _cmdErrmsg );
                    warning() << "got error doing chunk migrate: " << _cmdErrmsg << endl;
                }
                    
                configServer.logChange( (string)"moveChunk." + _where , _ns, _b.obj() );
            }
            catch ( const std::exception& e ) {
                warning() << "couldn't record timing for moveChunk '" << _where << "': " << e.what() << migrateLog;
            }
        }

        void done( int step ) {
            verify( step == ++_next );
            verify( step <= _total );

            stringstream ss;
            ss << "step" << step << " of " << _total;
            string s = ss.str();

            CurOp * op = cc().curop();
            if ( op )
                op->setMessage( s.c_str() );
            else
                warning() << "op is null in MoveTimingHelper::done" << migrateLog;

            _b.appendNumber( s , _t.millis() );
            _t.reset();

#if 0
            // debugging for memory leak?
            ProcessInfo pi;
            ss << " v:" << pi.getVirtualMemorySize()
               << " r:" << pi.getResidentSize();
            log() << ss.str() << migrateLog;
#endif
        }


        void note( const string& s ) {
            string field = "note";
            if ( _nextNote > 0 ) {
                StringBuilder buf;
                buf << "note" << _nextNote;
                field = buf.str();
            }
            _nextNote++;

            _b.append( field , s );
        }

    private:
        Timer _t;

        string _where;
        string _ns;

        int _next;
        int _total; // expected # of steps
        int _nextNote;

        string _cmdErrmsg;

        BSONObjBuilder _b;

    };

    class ChunkCommandHelper : public Command {
    public:
        ChunkCommandHelper( const char * name )
            : Command( name ) {
        }

        virtual void help( stringstream& help ) const {
            help << "internal - should not be called directly";
        }
        virtual bool slaveOk() const { return false; }
        virtual bool requiresShardedOperationScope() const { return false; }
        virtual bool adminOnly() const { return true; }
        virtual LockType locktype() const { return OPLOCK; }
        virtual bool requiresSync() const { return false; }
        virtual bool needsTxn() const { return false; }
        virtual int txnFlags() const { return noTxnFlags(); }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual OpSettings getOpSettings() const { return OpSettings(); }
    };

    bool isInRange( const BSONObj& obj ,
                    const BSONObj& min ,
                    const BSONObj& max ,
                    const BSONObj& shardKeyPattern ) {
        ShardKeyPattern shardKey( shardKeyPattern );
        BSONObj k = shardKey.extractKey( obj );
        return k.woCompare( min ) >= 0 && k.woCompare( max ) < 0;
    }


    class MigrateFromStatus {
    public:

        MigrateFromStatus()
                : _mutex("MigrateFromStatus"),
                  _inCriticalSection(false),
                  _active(false),
                  _migrateLogCollection(NULL),
                  _migrateLogRefCollection(NULL),
                  _nextMigrateLogId(0),
                  _snapshotTaken(false) {}

        /**
         * @return false if cannot start. One of the reason for not being able to
         *     start is there is already an existing migration in progress.
         */
        bool start( const std::string& ns ,
                    const BSONObj& min ,
                    const BSONObj& max ,
                    const BSONObj& shardKeyPattern ) {
            scoped_lock l(_mutex); // reads and writes _active

            if (_active) {
                return false;
            }

            verify( ! min.isEmpty() );
            verify( ! max.isEmpty() );
            verify( ns.size() );

            _ns = ns;
            _min = min;
            _max = max;
            _shardKeyPattern = shardKeyPattern;

            _snapshotTaken = false;
            clearMigrateLog();
            _active = true;
            return true;
        }

        void done(bool inDestructor) {
            log() << "MigrateFromStatus::done About to acquire sharding write lock to exit critical "
                    "section" << endl;
            ShardingState::SetVersionScope sc;
            log() << "MigrateFromStatus::done write lock acquired" << endl;

            scoped_lock l(_mutex);
            disableLogTxnOpsForSharding();
            _snapshotTaken = false;
            if (!inDestructor) {
                // If we're in the destructor, don't bother because this might throw.  This is an
                // optimization that isn't necessary for correctness; next time we start a migration
                // we'll clean it up then if we don't do it now.  The normal (non-error) path always
                // cleans this up because we call done() explicitly.
                clearMigrateLog();
            }
            _active = false;
            _inCriticalSection = false;
            _inCriticalSectionCV.notify_all();
        }

        void clearMigrateLog() {
            string err;
            BSONObjBuilder res;
            LOCK_REASON(lockReason, "sharding: clearing migratelog");
            try {
                Client::WriteContext ctx(MIGRATE_LOG_NS, lockReason);
                Client::Transaction txn(DB_SERIALIZABLE);
                Collection *cl = getCollection(MIGRATE_LOG_NS);
                if (cl != NULL) {
                    cl->drop(err, res, false);
                }
                _nextMigrateLogId.store(0);
                _nextIdToTransfer = 0;
                _nextRefSeqToTransfer = 0;
                _migrateLogCollection = getOrCreateCollection(MIGRATE_LOG_NS, false);
                verify(_migrateLogCollection != NULL);
                txn.commit();
            }
            catch (DBException &e) {
                stringstream ss;
                ss << "Error clearing " << MIGRATE_LOG_NS << " to prepare for chunk migration."
                   << " err: " << err
                   << " res: " << res.obj()
                   << " exc: " << e.what();
                problem() << ss.str() << endl;
                throw;
            }
            try {
                Client::WriteContext ctx(MIGRATE_LOG_REF_NS, lockReason);
                Client::Transaction txn(DB_SERIALIZABLE);
                Collection *cl = getCollection(MIGRATE_LOG_REF_NS);
                if (cl != NULL) {
                    cl->drop(err, res, false);
                }
                _migrateLogRefCollection = getOrCreateCollection(MIGRATE_LOG_REF_NS, false);
                verify(_migrateLogRefCollection != NULL);
                txn.commit();
            }
            catch (DBException &e) {
                stringstream ss;
                ss << "Error clearing " << MIGRATE_LOG_REF_NS << " to prepare for chunk migration."
                   << " err: " << err
                   << " res: " << res.obj()
                   << " exc: " << e.what();
                problem() << ss.str() << endl;
                throw;
            }
        }

        bool shouldLogOp(const char *opstr, const char *ns, const BSONObj &obj) {
            if (!_getActive()) {
                return false;
            }

            if (!mongoutils::str::equals(_ns.c_str(), ns)) {
                return false;
            }

            massert(16781, mongoutils::str::stream()
                    << "a capped collection is being sharded, this should not happen"
                    << " ns: " << ns
                    << " opstr: " << opstr,
                    !OplogHelpers::invalidOpForSharding(opstr));

            if (!_snapshotTaken) {
                return false;
            }

            if (OplogHelpers::shouldLogOpForSharding(opstr)) {
                return isInRange(obj, _min, _max, _shardKeyPattern);
            }
            return false;
        }

        bool shouldLogUpdateOp(const char *opstr, const char *ns, const BSONObj &oldObj) {
            // Just a sanity check.  ShardStrategy::_prepareUpdate() appears to prevent you from updating a document such that the shard key changes.
            // We just verify that old and new have the same shard key, and pass to the normal case.
            // But we call shouldLogOp first to avoid doing the comparison if, say, we're in the wrong ns and we can stop early.
            return shouldLogOp(opstr, ns, oldObj);
        }

        void startObjForMigrateLog(BSONObjBuilder &b) {
            b << "_id" << _nextMigrateLogId.fetchAndAdd(1);
        }

        void writeObjToMigrateLog(BSONObj &obj) {
            LOCK_REASON(lockReason, "sharding: writing to migratelog");
            Client::ReadContext ctx(MIGRATE_LOG_NS, lockReason);
            insertOneObject(_migrateLogCollection, obj,
                            Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE);
        }

        void writeObjToMigrateLogRef(BSONObj &obj) {
            LOCK_REASON(lockReason, "sharding: writing to migratelog.refs");
            Client::ReadContext ctx(MIGRATE_LOG_REF_NS, lockReason);
            insertOneObject(_migrateLogRefCollection, obj,
                            Collection::NO_UNIQUE_CHECKS | Collection::NO_LOCKTREE);
        }

        /**
         * called from the dest of a migrate
         * transfers mods from src to dest
         */
        bool transferMods( string& errmsg , BSONObjBuilder& b ) {
            if ( ! _getActive() ) {
                errmsg = "no active migration!";
                return false;
            }

            BSONArrayBuilder arr(b.subarrayStart("mods"));
            const long long maxSize = 1024 * 1024;

            long long nextMigrateLogId = _nextMigrateLogId.load();
            dassert(nextMigrateLogId >= _nextIdToTransfer);

            if (nextMigrateLogId > _nextIdToTransfer) {
                DBDirectClient conn;
                auto_ptr<DBClientCursor> cur(conn.query(MIGRATE_LOG_NS, QUERY("_id" << GTE << _nextIdToTransfer << LT << nextMigrateLogId).hint(BSON("_id" << 1))));
                while (cur->more()) {
                    BSONObj obj = cur->next();
                    BSONElement refOID = obj["refOID"];
                    if (refOID.ok()) {
                        auto_ptr<DBClientCursor> refcur(conn.query(MIGRATE_LOG_REF_NS, QUERY("_id.oid" << refOID.OID() << "_id.seq" << GTE << _nextRefSeqToTransfer).hint(BSON("_id" << 1))));
                        bool didBreak = false;
                        while (refcur->more()) {
                            BSONObj refObj = refcur->next();
                            if (arr.len() + refObj.objsize() > maxSize) {
                                didBreak = true;
                                break;
                            }
                            _nextRefSeqToTransfer = refObj["_id"]["seq"].numberLong() + 1;
                            arr.append(refObj);
                        }
                        if (!didBreak) {
                            // Exhausted that ref object naturally.
                            _nextIdToTransfer = obj["_id"].numberLong() + 1;
                            _nextRefSeqToTransfer = 0;
                        }
                    }
                    else {
                        if (arr.len() + obj.objsize() > maxSize) {
                            break;
                        }
                        _nextIdToTransfer = obj["_id"].numberLong() + 1;
                        _nextRefSeqToTransfer = 0;
                        arr.append(obj);
                    }
                }
            }

            arr.done();
            return true;
        }

        /**
         * Start a transaction that will be used for cloning by the recipient, and start building the migratelog.
         * This needs to be exclusive with the old clone path, _migrateClone, and we sadly need to support both in the case of mixed-version clusters.
         */
        CursorId startCloneTransaction(const BSONObj &cmdobj, string &errmsg, BSONObjBuilder &result) {
            string ns(cmdobj["ns"].String());
            LOCK_REASON(lockReason, "sharding: starting clone transaction for migrate");
            Client::ReadContext ctx(ns, lockReason);
            massert(17223, "can't _migrateStartCloneTransaction with active snapshot", !_snapshotTaken);

            Collection *cl = getCollection(ns);
            if (cl == NULL) {
                errmsg = "collection not found, should be impossible";
                return 0;
            }

            const IndexDetails *idx = cl->findIndexByPrefix(cmdobj["keyPattern"].Obj(), true);
            if (idx == NULL) {
                errmsg = mongoutils::str::stream() << "can't find index for " << cmdobj["keyPattern"].Obj() << " in _migrateStartCloneTransaction";
                return 0;
            }

            KeyPattern kp(idx->keyPattern());
            BSONObj min = KeyPattern::toKeyFormat(kp.extendRangeBound(cmdobj["min"].Obj(), false));
            BSONObj max = KeyPattern::toKeyFormat(kp.extendRangeBound(cmdobj["max"].Obj(), false));

            // This transaction will be used to associate the row locks we take below, until after
            // we create the transaction and cursor we'll use later to pull all the data over.
            Client::Transaction lockingTxn(DB_SERIALIZABLE | DB_TXN_READ_ONLY);

            {
                // Need these OpSettings to make sure this cursor will prelock and take row locks.
                // Also need numWanted == 0 in Cursor::make.
                Client::WithOpSettings wos(OpSettings().setQueryCursorMode(READ_LOCK_CURSOR).setJustOne(false));
                // This lock may take a while to grab, we want to try for a while to get it.
                Client::WithLockTimeout wlt(migrateStartCloneLockTimeout);

                // Just creating the cursor under these conditions will prelock the range.
                // After this point (until lockingTxn commits), nothing else can write to our chunk, so we can create the snapshot.
                // Note that this cursor "looks like" the cursor we'll use to read the chunk, in terms of startKey, endKey, and endKeyInclusive.
                Cursor::make(cl, *idx, min, max, false, 1);

                enableLogTxnOpsForSharding(mongo::shouldLogOpForSharding,
                                           mongo::shouldLogUpdateOpForSharding,
                                           mongo::startObjForMigrateLog,
                                           mongo::writeObjToMigrateLog,
                                           mongo::writeObjToMigrateLogRef);
                _snapshotTaken = true;
            }

            // Need to avoid committing the above transaction until after we've created our cursor,
            // so we'll make an AlternateTransactionStack here; this transaction will do the real
            // work after we commit the above transaction, using this cursor.
            CursorId cursorid;
            {
                Client::AlternateTransactionStack ats;
                Client::Transaction txn(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
                cc().setOpSettings(OpSettings().setQueryCursorMode(DEFAULT_LOCK_CURSOR).setBulkFetch(true));

                ClientCursor::Holder ccPointer(new ClientCursor(0, Cursor::make(cl, *idx, min, max, false, 1), ns, cmdobj.getOwned()));
                cursorid = ccPointer->cursorid();
                cc().swapTransactionStack(ccPointer->transactions);
                ccPointer.release();
            }

            lockingTxn.commit();
            return cursorid;
        }

        /**
         * Get the BSONs that belong to the chunk migrated in shard key order.
         *
         * @param maxChunkSize number of bytes beyond which a chunk's base data (no indices) is considered too large to move
         * @param errmsg filled with textual description of error if this call return false
         * @return false if approximate chunk size is too big to move or true otherwise
         */
        bool clone(string& errmsg , BSONObjBuilder& result ) {
            if ( ! _getActive() ) {
                errmsg = "not active";
                return false;
            }

            Collection *cl;
            if (_cc.get() == NULL) {
                dassert(!_txn);
                LOCK_REASON(lockReason, "sharding: enabling migratelog");
                Client::WriteContext ctx(_ns, lockReason);
                massert(17224, "can't start _migrateClone with active snapshot", !_snapshotTaken);
                enableLogTxnOpsForSharding(mongo::shouldLogOpForSharding,
                                           mongo::shouldLogUpdateOpForSharding,
                                           mongo::startObjForMigrateLog,
                                           mongo::writeObjToMigrateLog,
                                           mongo::writeObjToMigrateLogRef);
                _snapshotTaken = true;
                _txn.reset(new Client::Transaction(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY));

                cl = getCollection(_ns);
                if (cl == NULL) {
                    errmsg = "ns not found, should be impossible";
                    _txn.reset();
                    return false;
                }

                const IndexDetails *idx = cl->findIndexByPrefix( _shardKeyPattern ,
                                                                true );  /* require single key */

                if ( idx == NULL ) {
                    errmsg = mongoutils::str::stream() << "can't find index for " << _shardKeyPattern << " in _migrateClone" << causedBy( errmsg );
                    _txn.reset();
                    return false;
                }
                // Assume both min and max non-empty, append MinKey's to make them fit chosen index
                KeyPattern kp( idx->keyPattern() );
                BSONObj min = KeyPattern::toKeyFormat( kp.extendRangeBound( _min, false ) );
                BSONObj max = KeyPattern::toKeyFormat( kp.extendRangeBound( _max, false ) );

                shared_ptr<Cursor> idxCursor(Cursor::make( cl , *idx , min , max , false , 1 ));
                _cc.reset(new ClientCursor(QueryOption_NoCursorTimeout, idxCursor, _ns));

                cc().swapTransactionStack(_txnStack);
            }

            {
                Client::WithTxnStack wts(_txnStack);
                LOCK_REASON(lockReason, "sharding: cloning documents from donor for migrate");
                Client::ReadContext ctx(_ns, lockReason);

                BSONArrayBuilder a(result.subarrayStart("objects"));

                bool empty = true;
                for (; _cc->ok(); _cc->advance()) {
                    BSONObj obj = _cc->current();
                    if (a.arrSize() > 0 &&
                        result.len() + obj.objsize() + 1024 >= BSONObjMaxUserSize) {
                        // have to do another batch after this
                        break;
                    }
                    a.append(obj);
                    empty = false;
                }

                a.doneFast();

                if (empty) {
                    _cc.reset();
                    _txn->commit();
                    _txn.reset();
                }
            }

            return true;
        }

        bool getInCriticalSection() const {
            scoped_lock l(_mutex);
            return _inCriticalSection;
        }

        void setInCriticalSection( bool b ) {
            scoped_lock l(_mutex);
            _inCriticalSection = b;
            _inCriticalSectionCV.notify_all();
        }

        /**
         * @return true if we are NOT in the critical section
         */
        bool waitTillNotInCriticalSection( int maxSecondsToWait ) {
            verify( !Lock::isLocked() );

            boost::xtime xt;
            boost::xtime_get(&xt, MONGO_BOOST_TIME_UTC);
            xt.sec += maxSecondsToWait;

            scoped_lock l(_mutex);
            while ( _inCriticalSection ) {
                if ( ! _inCriticalSectionCV.timed_wait( l.boost(), xt ) )
                    return false;
            }

            return true;
        }

        bool isActive() const { return _getActive(); }
        
        static const char MIGRATE_LOG_NS[];
        static const char MIGRATE_LOG_REF_NS[];
    private:
        mutable mongo::mutex _mutex; // protect _inCriticalSection and _active
        boost::condition _inCriticalSectionCV;

        bool _inCriticalSection;
        bool _active;

        string _ns;
        BSONObj _min;
        BSONObj _max;
        BSONObj _shardKeyPattern;

        Collection *_migrateLogCollection;
        Collection *_migrateLogRefCollection;
        AtomicWord<long long> _nextMigrateLogId;
        long long _nextIdToTransfer;
        long long _nextRefSeqToTransfer;

        bool _snapshotTaken;
        scoped_ptr<Client::Transaction> _txn;
        shared_ptr<Client::TransactionStack> _txnStack;
        auto_ptr<ClientCursor> _cc;

        bool _getActive() const { scoped_lock l(_mutex); return _active; }
        void _setActive( bool b ) { scoped_lock l(_mutex); _active = b; }

    } migrateFromStatus;

    const char MigrateFromStatus::MIGRATE_LOG_NS[] = "local.migratelog.sh";
    const char MigrateFromStatus::MIGRATE_LOG_REF_NS[] = "local.migratelogref.sh";

    struct MigrateStatusHolder {
        MigrateStatusHolder( const std::string& ns ,
                             const BSONObj& min ,
                             const BSONObj& max ,
                             const BSONObj& shardKeyPattern ) {
            _isAnotherMigrationActive = !migrateFromStatus.start(ns, min, max, shardKeyPattern);
        }
        ~MigrateStatusHolder() {
            if (!_isAnotherMigrationActive) {
                migrateFromStatus.done(true);
            }
        }

        bool isAnotherMigrationActive() const {
            return _isAnotherMigrationActive;
        }

    private:
        bool _isAnotherMigrationActive;
    };

    bool shouldLogOpForSharding(const char *opstr, const char *ns, const BSONObj &obj) {
        return migrateFromStatus.shouldLogOp(opstr, ns, obj);
    }

    bool shouldLogUpdateOpForSharding(const char *opstr, const char *ns, const BSONObj &oldObj) {
        return migrateFromStatus.shouldLogUpdateOp(opstr, ns, oldObj);
    }

    void startObjForMigrateLog(BSONObjBuilder &b) {
        migrateFromStatus.startObjForMigrateLog(b);
    }

    void writeObjToMigrateLog(BSONObj &obj) {
        migrateFromStatus.writeObjToMigrateLog(obj);
    }

    void writeObjToMigrateLogRef(BSONObj &obj) {
        migrateFromStatus.writeObjToMigrateLogRef(obj);
    }

    class TransferModsCommand : public ChunkCommandHelper {
    public:
        TransferModsCommand() : ChunkCommandHelper( "_transferMods" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_transferMods);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            return migrateFromStatus.transferMods( errmsg, result );
        }
    } transferModsCommand;

    class StartCloneTransactionCommand : public ChunkCommandHelper {
    public:
        StartCloneTransactionCommand() : ChunkCommandHelper( "_migrateStartCloneTransaction" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_migrateStartCloneTransaction);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            CursorId id = migrateFromStatus.startCloneTransaction(cmdObj, errmsg, result);
            if (id == 0) {
                return false;
            }

            // This is effectively a "getMore" so it should have a read lock.
            LOCK_REASON(lockReason, "sharding: getting first batch while starting clone transaction for migrate");
            Client::ReadContext ctx(cmdObj["ns"].Stringdata(), lockReason);
            ClientCursor::Pin pin(id);
            ClientCursor *cursor = pin.c();
            massert(17227, "Cursor shouldn't have been deleted", cursor);

            BufBuilder &bb = result.bb();
            int lenBefore = bb.len();
            try {
                cc().setOpSettings(OpSettings().setQueryCursorMode(DEFAULT_LOCK_CURSOR).setBulkFetch(true));
                Client::WithTxnStack wts(cursor->transactions);
                BSONObjBuilder cursorObj(result.subobjStart("cursor"));
                cursorObj.append("id", id);
                cursorObj.append("ns", cmdObj["ns"].Stringdata());
                BSONArrayBuilder ab(cursorObj.subarrayStart("firstBatch"));
                for (; cursor->ok(); cursor->advance()) {
                    BSONObj obj = cursor->current();
                    if (obj.objsize() + ab.len() >= MaxBytesToReturnToClientAtOnce) {
                        break;
                    }
                    ab.append(obj);
                }
                ab.done();
                cursorObj.done();
                cursor->resetIdleAge();
                return true;
            }
            catch (...) {
                // Clean up anything we may have added to the result already.
                bb.setlen(lenBefore);
                pin.release();
                ClientCursor::erase(id);
                throw;
            }
        }
    } startCloneTransactionCommand;

    class InitialCloneCommand : public ChunkCommandHelper {
    public:
        InitialCloneCommand() : ChunkCommandHelper( "_migrateClone" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_migrateClone);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            return migrateFromStatus.clone( errmsg, result );
        }
    } initialCloneCommand;

    void cleanupOldData(const string &ns, const BSONObj &shardKeyPattern, const BSONObj &min, const BSONObj &max) {
        LOG(0) << "moveChunk starting delete for: " << ns << " from " << min << " -> " << max << migrateLog;
        {
            ShardForceVersionOkModeBlock sf;
            LOCK_REASON(lockReason, "sharding: deleting old documents after migrate");
            Client::ReadContext ctx(ns, lockReason);

            BSONObj indexKeyPattern;
            if (!findShardKeyIndexPattern_locked(ns, shardKeyPattern, &indexKeyPattern)) {
                warning() << "collection or index dropped before data could be cleaned" << endl;
                return;
            }

            Client::Transaction txn(DB_SERIALIZABLE);
            long long numDeleted =
                    deleteIndexRange(ns,
                                     min,
                                     max,
                                     indexKeyPattern,
                                     false, /*maxInclusive*/
                                     true,  /*fromMigrate*/
                                     Collection::NO_LOCKTREE);
            txn.commit();

            LOG(0) << "moveChunk deleted " << numDeleted << " documents for "
                   << ns << " from " << min << " -> " << max << migrateLog;
        }

        GTID lastGTID = cc().getLastOp();
        Timer t;
        for (int i=0; i<3600; i++) {
            if (opReplicatedEnough(lastGTID, (getSlaveCount() / 2) + 1)) {
                LOG(t.seconds() < 30 ? 1 : 0) << "moveChunk repl sync took " << t.seconds() << " seconds" << migrateLog;
                return;
            }
            sleepsecs(1);
        }

        warning() << "moveChunk repl sync timed out after " << t.seconds() << " seconds" << migrateLog;
    }

    /**
     * this is the main entry for moveChunk
     * called to initial a move
     * usually by a mongos
     * this is called on the "from" side
     */
    class MoveChunkCommand : public Command {
    public:
        MoveChunkCommand() : Command( "moveChunk" ) {}
        virtual void help( stringstream& help ) const {
            help << "should not be calling this directly";
        }

        virtual bool slaveOk() const { return false; }
        virtual bool adminOnly() const { return true; }
        virtual LockType locktype() const { return OPLOCK; }
        virtual bool requiresShardedOperationScope() const { return false; }
        // this makes so little sense...
        virtual bool requiresSync() const { return false; }
        virtual bool needsTxn() const { return false; }
        virtual int txnFlags() const { return noTxnFlags(); }
        virtual bool canRunInMultiStmtTxn() const { return false; }
        virtual OpSettings getOpSettings() const { return OpSettings(); }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::moveChunk);
            out->push_back(Privilege(AuthorizationManager::CLUSTER_RESOURCE_NAME, actions));
        }

        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            // 1. parse options
            // 2. make sure my view is complete and lock
            // 3. start migrate
            //    in a read lock, get all primary keys and sort so we can do as little seeking as possible
            //    tell to start transferring
            // 4. pause till migrate caught up
            // 5. LOCK
            //    a) update my config, essentially locking
            //    b) finish migrate
            //    c) update config server
            //    d) logChange to config server
            // 6. wait for all current cursors to expire
            // 7. remove data locally

            // -------------------------------

            // 1.
            string ns = cmdObj.firstElement().str();
            string to = cmdObj["to"].str();
            string from = cmdObj["from"].str(); // my public address, a tad redundant, but safe

            // fromShard and toShard needed so that 2.2 mongos can interact with either 2.0 or 2.2 mongod
            if( cmdObj["fromShard"].type() == String ){
                from = cmdObj["fromShard"].String();
            }

            if( cmdObj["toShard"].type() == String ){
                to = cmdObj["toShard"].String();
            }

            BSONObj min  = cmdObj["min"].Obj();
            BSONObj max  = cmdObj["max"].Obj();
            BSONElement shardId = cmdObj["shardId"];

            if ( ns.empty() ) {
                errmsg = "need to specify namespace in command";
                return false;
            }

            if ( to.empty() ) {
                errmsg = "need to specify shard to move chunk to";
                return false;
            }
            if ( from.empty() ) {
                errmsg = "need to specify shard to move chunk from";
                return false;
            }

            if ( min.isEmpty() ) {
                errmsg = "need to specify a min";
                return false;
            }

            if ( max.isEmpty() ) {
                errmsg = "need to specify a max";
                return false;
            }

            if ( shardId.eoo() ) {
                errmsg = "need shardId";
                return false;
            }

            if ( ! shardingState.enabled() ) {
                if ( cmdObj["configdb"].type() != String ) {
                    errmsg = "sharding not enabled";
                    return false;
                }
                string configdb = cmdObj["configdb"].String();
                ShardingState::initialize(configdb);
            }

            MoveTimingHelper timing( "from" , ns , min , max , 6 /* steps */ , errmsg );

            // Make sure we're as up-to-date as possible with shard information
            // This catches the case where we had to previously changed a shard's host by
            // removing/adding a shard with the same name
            Shard::reloadShardInfo();

            // So 2.2 mongod can interact with 2.0 mongos, mongod needs to handle either a conn
            // string or a shard in the to/from fields.  The Shard constructor handles this,
            // eventually we should break the compatibility.

            Shard fromShard( from );
            Shard toShard( to );

            log() << "received moveChunk request: " << cmdObj << migrateLog;

            timing.done(1);

            // 2.
            
            if ( migrateFromStatus.isActive() ) {
                errmsg = "migration already in progress";
                return false;
            }

            DistributedLock lockSetup( ConnectionString( shardingState.getConfigServer() , ConnectionString::SYNC ) , ns );
            dist_lock_try dlk;

            try{
                dlk = dist_lock_try( &lockSetup , (string)"migrate-" + min.toString(), 30.0 /*timeout*/ );
            }
            catch( LockException& e ){
                errmsg = str::stream() << "error locking distributed lock for migration " << "migrate-" << min.toString() << causedBy( e );
                return false;
            }

            if ( ! dlk.got() ) {
                errmsg = str::stream() << "the collection metadata could not be locked with lock " << "migrate-" << min.toString();
                result.append( "who" , dlk.other() );
                return false;
            }

            BSONObj chunkInfo = BSON("min" << min << "max" << max << "from" << fromShard.getName() << "to" << toShard.getName() );
            configServer.logChange( "moveChunk.start" , ns , chunkInfo );

            ChunkVersion maxVersion;
            ChunkVersion startingVersion;
            string myOldShard;
            {
                scoped_ptr<ScopedDbConnection> conn(
                        ScopedDbConnection::getInternalScopedDbConnection(
                                shardingState.getConfigServer(), 30));

                BSONObj x;
                BSONObj currChunk;
                try{
                    x = conn->get()->findOne(ChunkType::ConfigNS,
                                             Query(BSON(ChunkType::ns(ns)))
                                                  .sort(BSON(ChunkType::DEPRECATED_lastmod() << -1)));

                    currChunk = conn->get()->findOne(ChunkType::ConfigNS,
                                                     shardId.wrap(ChunkType::name().c_str()));
                }
                catch( DBException& e ){
                    errmsg = str::stream() << "aborted moveChunk because could not get chunk data from config server " << shardingState.getConfigServer() << causedBy( e );
                    warning() << errmsg << endl;
                    return false;
                }

                maxVersion = ChunkVersion::fromBSON(x, ChunkType::DEPRECATED_lastmod());
                verify(currChunk[ChunkType::shard()].type());
                verify(currChunk[ChunkType::min()].type());
                verify(currChunk[ChunkType::max()].type());
                myOldShard = currChunk[ChunkType::shard()].String();
                conn->done();

                BSONObj currMin = currChunk[ChunkType::min()].Obj();
                BSONObj currMax = currChunk[ChunkType::max()].Obj();
                if ( currMin.woCompare( min ) || currMax.woCompare( max ) ) {
                    errmsg = "boundaries are outdated (likely a split occurred)";
                    result.append( "currMin" , currMin );
                    result.append( "currMax" , currMax );
                    result.append( "requestedMin" , min );
                    result.append( "requestedMax" , max );

                    warning() << "aborted moveChunk because" <<  errmsg << ": " << min << "->" << max
                                      << " is now " << currMin << "->" << currMax << migrateLog;
                    return false;
                }

                if ( myOldShard != fromShard.getName() ) {
                    errmsg = "location is outdated (likely balance or migrate occurred)";
                    result.append( "from" , fromShard.getName() );
                    result.append( "official" , myOldShard );

                    warning() << "aborted moveChunk because " << errmsg << ": chunk is at " << myOldShard
                                      << " and not at " << fromShard.getName() << migrateLog;
                    return false;
                }

                if ( maxVersion < shardingState.getVersion( ns ) ) {
                    errmsg = "official version less than mine?";
                    maxVersion.addToBSON( result, "officialVersion" );
                    shardingState.getVersion( ns ).addToBSON( result, "myVersion" );

                    warning() << "aborted moveChunk because " << errmsg << ": official " << maxVersion
                                      << " mine: " << shardingState.getVersion(ns) << migrateLog;
                    return false;
                }

                // since this could be the first call that enable sharding we also make sure to have the chunk manager up to date
                shardingState.gotShardName( myOldShard );

                // Using the maxVersion we just found will enforce a check - if we use zero version,
                // it's possible this shard will be *at* zero version from a previous migrate and
                // no refresh will be done
                // TODO: Make this less fragile
                startingVersion = maxVersion;
                shardingState.trySetVersion( ns , startingVersion /* will return updated */ );

                if (startingVersion.majorVersion() == 0) {
                   // It makes no sense to migrate if our version is zero and we have no chunks, so return
                   warning() << "moveChunk cannot start migration with zero version" << endl;
                   return false;
                }

                log() << "moveChunk request accepted at version " << startingVersion << migrateLog;
            }

            timing.done(2);

            // 3.

            ShardChunkManagerPtr chunkManager = shardingState.getShardChunkManager( ns );
            verify( chunkManager != NULL );
            BSONObj shardKeyPattern = chunkManager->getKey();
            if ( shardKeyPattern.isEmpty() ){
                errmsg = "no shard key found";
                return false;
            }

            MigrateStatusHolder statusHolder( ns , min , max , shardKeyPattern );
            if (statusHolder.isAnotherMigrationActive()) {
                errmsg = "moveChunk is already in progress from this shard";
                return false;
            }

            {
                scoped_ptr<ScopedDbConnection> connTo(
                        ScopedDbConnection::getScopedDbConnection( toShard.getConnString() ) );
                BSONObj res;
                bool ok;
                try{
                    ok = connTo->get()->runCommand( "admin" ,
                                                    BSON( "_recvChunkStart" << ns <<
                                                          "from" << fromShard.getConnString() <<
                                                          "min" << min <<
                                                          "max" << max <<
                                                          "shardKeyPattern" << shardKeyPattern <<
                                                          "configServer" << configServer.modelServer()
                                                          ) ,
                                                    res );
                }
                catch( DBException& e ){
                    errmsg = str::stream() << "moveChunk could not contact to: shard "
                                           << to << " to start transfer" << causedBy( e );
                    warning() << errmsg << endl;
                    return false;
                }

                connTo->done();

                if ( ! ok ) {
                    errmsg = "moveChunk failed to engage TO-shard in the data transfer: ";
                    verify( res["errmsg"].type() );
                    errmsg += res["errmsg"].String();
                    result.append( "cause" , res );
                    warning() << errmsg << endl;
                    return false;
                }

            }
            timing.done( 3 );

            // 4.
            for ( int i=0; i<86400; i++ ) { // don't want a single chunk move to take more than a day
                verify( !Lock::isLocked() );
                // Exponential sleep backoff, up to 1024ms. Don't sleep much on the first few
                // iterations, since we want empty chunk migrations to be fast.
                sleepmillis( 1 << std::min( i , 10 ) );
                scoped_ptr<ScopedDbConnection> conn(
                        ScopedDbConnection::getScopedDbConnection( toShard.getConnString() ) );
                BSONObj res;
                bool ok;
                try {
                    ok = conn->get()->runCommand( "admin" , BSON( "_recvChunkStatus" << 1 ) , res );
                    res = res.getOwned();
                }
                catch( DBException& e ){
                    errmsg = str::stream() << "moveChunk could not contact to: shard " << to << " to monitor transfer" << causedBy( e );
                    warning() << errmsg << endl;
                    return false;
                }

                conn->done();

                LOG(0) << "moveChunk data transfer progress: " << res << migrateLog;

                if ( ! ok || res["state"].String() == "fail" ) {
                    warning() << "moveChunk error transferring data caused migration abort: " << res << migrateLog;
                    errmsg = "data transfer error";
                    result.append( "cause" , res );
                    return false;
                }

                if ( res["state"].String() == "steady" )
                    break;

                killCurrentOp.checkForInterrupt();
            }
            timing.done(4);

            // 5.

            // Before we get into the critical section of the migration, let's double check
            // that the config servers are reachable and the lock is in place.
            log() << "About to check if it is safe to enter critical section";

            string lockHeldMsg;
            bool lockHeld = dlk.isLockHeld( 30.0 /* timeout */, &lockHeldMsg );
            if ( !lockHeld ) {
                errmsg = str::stream() << "not entering migrate critical section because "
                                       << lockHeldMsg;
                warning() << errmsg << endl;
                return false;
            }

            log() << "About to enter migrate critical section";

            {
                // 5.a
                // we're under the collection lock here, so no other migrate can change maxVersion or ShardChunkManager state
                migrateFromStatus.setInCriticalSection( true );
                ChunkVersion myVersion = maxVersion;
                myVersion.incMajor();

                {
                    ShardingState::SetVersionScope sc;
                    verify( myVersion > shardingState.getVersion( ns ) );

                    // bump the chunks manager's version up and "forget" about the chunk being moved
                    // this is not the commit point but in practice the state in this shard won't until the commit it done
                    shardingState.donateChunk( ns , min , max , myVersion );
                }

                log() << "moveChunk setting version to: " << myVersion << migrateLog;

                // 5.b
                // we're under the collection lock here, too, so we can undo the chunk donation because no other state change
                // could be ongoing

                BSONObj res;
                bool ok;

                try {
                    // This timeout (330 seconds) is bigger than on vanilla mongodb, since the
                    // transferMods we have to do even though we think we're in a steady state could
                    // be much larger than in vanilla.
                    scoped_ptr<ScopedDbConnection> connTo(
                            ScopedDbConnection::getScopedDbConnection( toShard.getConnString(),
                                                                       330.0 ) );

                    ok = connTo->get()->runCommand( "admin", BSON( "_recvChunkCommit" << 1 ), res );
                    connTo->done();
                }
                catch( DBException& e ){
                    errmsg = str::stream() << "moveChunk could not contact to: shard "
                                           << toShard.getConnString() << " to commit transfer"
                                           << causedBy( e );
                    warning() << errmsg << endl;
                    ok = false;
                }

                if ( !ok ) {
                    log() << "moveChunk migrate commit not accepted by TO-shard: " << res
                          << " resetting shard version to: " << startingVersion << migrateLog;
                    {
                        ShardingState::SetVersionScope sc;
                        log() << "moveChunk global lock acquired to reset shard version from "
                              "failed migration"
                              << endl;

                        // revert the chunk manager back to the state before "forgetting" about the
                        // chunk
                        shardingState.undoDonateChunk( ns, min, max, startingVersion );
                    }
                    log() << "Shard version successfully reset to clean up failed migration"
                          << endl;

                    errmsg = "_recvChunkCommit failed!";
                    result.append( "cause", res );
                    return false;
                }

                log() << "moveChunk migrate commit accepted by TO-shard: " << res << migrateLog;

                // 5.c

                // version at which the next highest lastmod will be set
                // if the chunk being moved is the last in the shard, nextVersion is that chunk's lastmod
                // otherwise the highest version is from the chunk being bumped on the FROM-shard
                ChunkVersion nextVersion;

                // we want to go only once to the configDB but perhaps change two chunks, the one being migrated and another
                // local one (so to bump version for the entire shard)

                try {
                    scoped_ptr<ScopedDbConnection> conn(ScopedDbConnection::getInternalScopedDbConnection(shardingState.getConfigServer(), 10.0));
                    scoped_ptr<RemoteTransaction> txn(new RemoteTransaction(conn->conn(), "serializable"));

                    // Check the precondition
                    BSONObjBuilder b;
                    b.appendTimestamp(ChunkType::DEPRECATED_lastmod(), maxVersion.toLong());
                    BSONObj expect = b.done();
                    Matcher m(expect);

                    BSONObj found = conn->get()->findOne(ChunkType::ConfigNS, QUERY(ChunkType::ns(ns)).sort(ChunkType::DEPRECATED_lastmod(), -1));
                    if (!m.matches(found)) {
                        // TODO(leif): Make sure that this means the sharding algorithm is broken and we should bounce the server.
                        error() << "moveChunk commit failed: " << ChunkVersion::fromBSON(found[ChunkType::DEPRECATED_lastmod()])
                                << " instead of " << maxVersion << migrateLog;
                        error() << "TERMINATING" << migrateLog;
                        dumpCrashInfo("moveChunk commit failed");
                        dbexit(EXIT_SHARDING_ERROR);
                    }

                    try {
                        // update for the chunk being moved
                        BSONObjBuilder n;
                        n.append(ChunkType::name(), Chunk::genID(ns, min));
                        myVersion.addToBSON(n, ChunkType::DEPRECATED_lastmod());
                        n.append(ChunkType::ns(), ns);
                        n.append(ChunkType::min(), min);
                        n.append(ChunkType::max(), max);
                        n.append(ChunkType::shard(), toShard.getName());
                        conn->get()->update(ChunkType::ConfigNS, QUERY(ChunkType::name() << Chunk::genID(ns, min)), n.done());
                    }
                    catch (DBException &e) {
                        warning() << e << migrateLog;
                        error() << "moveChunk error updating the chunk being moved" << migrateLog;
                        txn.reset();
                        conn->done();
                        throw;
                    }

                    nextVersion = myVersion;

                    // if we have chunks left on the FROM shard, update the version of one of them as well
                    // we can figure that out by grabbing the chunkManager installed on 5.a
                    // TODO expose that manager when installing it

                    ShardChunkManagerPtr chunkManager = shardingState.getShardChunkManager(ns);
                    if (chunkManager->getNumChunks() > 0) {
                        // get another chunk on that shard
                        BSONObj lookupKey;
                        BSONObj bumpMin, bumpMax;
                        do {
                            chunkManager->getNextChunk( lookupKey , &bumpMin , &bumpMax );
                            lookupKey = bumpMin;
                        }
                        while( bumpMin == min );

                        nextVersion.incMinor();  // same as used on donateChunk
                        try {
                            BSONObjBuilder n;
                            n.append(ChunkType::name(), Chunk::genID(ns, bumpMin));
                            nextVersion.addToBSON(n, ChunkType::DEPRECATED_lastmod());
                            n.append(ChunkType::ns(), ns);
                            n.append(ChunkType::min(), bumpMin);
                            n.append(ChunkType::max(), bumpMax);
                            n.append(ChunkType::shard(), fromShard.getName());
                            conn->get()->update(ChunkType::ConfigNS, QUERY(ChunkType::name() << Chunk::genID(ns, bumpMin)), n.done());
                            log() << "moveChunk updating self version to: " << nextVersion << " through "
                                  << bumpMin << " -> " << bumpMax << " for collection '" << ns << "'" << migrateLog;
                        }
                        catch (DBException &e) {
                            warning() << e << migrateLog;
                            error() << "moveChunk error updating chunk on the FROM shard" << migrateLog;
                            txn.reset();
                            conn->done();
                            throw;
                        }
                    }
                    else {
                        log() << "moveChunk moved last chunk out for collection '" << ns << "'" << migrateLog;
                    }

                    {
                        static const int max_commit_retries = 30;
                        int retries = max_commit_retries;
                        bool committed = false;
                        while (retries-- > 0) {
                            BSONObj res;
                            committed = txn->commit(&res);
                            if (committed) {
                                break;
                            }
                            if (res["code"].isNumber() && res["code"].numberLong() == storage::ASSERT_IDS::TxnNotFoundOnCommit) {
                                // It may have been committed without giving us a response, or it may have been aborted.
                                // We need to check whether the effect of our operation can be read.
                                //
                                // if the commit made it to the config, we'll see the chunk in the new shard and there's no action
                                // if the commit did not make it, currently the only way to fix this state is to bounce the mongod so
                                // that the old state (before migrating) be brought in

                                warning() << "moveChunk commit outcome ongoing" << migrateLog;

                                // look for the chunk in this shard whose version got bumped
                                // we assume that if that mod made it to the config, the transaction was successful
                                BSONObj doc = conn->get()->findOne(ChunkType::ConfigNS,
                                                                   Query(BSON(ChunkType::ns(ns)))
                                                                   .sort(BSON(ChunkType::DEPRECATED_lastmod() << -1)));

                                ChunkVersion checkVersion =
                                        ChunkVersion::fromBSON(doc[ChunkType::DEPRECATED_lastmod()]);

                                if (checkVersion.isEquivalentTo(nextVersion)) {
                                    log() << "moveChunk commit confirmed" << migrateLog;
                                    committed = true;
                                    break;
                                } else {
                                    error() << "moveChunk commit failed: version is at"
                                            << checkVersion << " instead of " << nextVersion << migrateLog;
                                    error() << "TERMINATING" << migrateLog;
                                    dbexit( EXIT_SHARDING_ERROR );
                                }
                            }

                            warning() << "Error committing transaction to finish migration: " << res << migrateLog;
                            warning() << "Retrying (" << retries << " attempts remaining)" << migrateLog;
                            sleepmillis(std::min(1 << (max_commit_retries - retries), 1000));
                        }
                        if (!committed) {
                            stringstream ss;
                            ss << "Couldn't commit transaction to finish migration after " << (max_commit_retries - retries) << " attempts.";
                            error() << ss.str() << migrateLog;
                            txn.reset();
                            conn->done();
                            msgasserted(17328, ss.str());
                        }
                    }
                    txn.reset();
                    conn->done();
                }
                catch (DBException& e) {
                    warning() << e << migrateLog;
                    int exceptionCode = e.getCode();

                    if (exceptionCode == PrepareConfigsFailedCode) {

                        // In the process of issuing the migrate commit, the SyncClusterConnection
                        // checks that the config servers are reachable. If they are not, we are
                        // sure that the applyOps command was not sent to any of the configs, so we
                        // can safely back out of the migration here, by resetting the shard
                        // version that we bumped up to in the donateChunk() call above.

                        log() << "About to acquire moveChunk global lock to reset shard version from "
                              << "failed migration" << endl;

                        {
                            ShardingState::SetVersionScope sc;

                            // Revert the chunk manager back to the state before "forgetting"
                            // about the chunk.
                            shardingState.undoDonateChunk(ns, min, max, startingVersion);
                        }

                        log() << "Shard version successfully reset to clean up failed migration" << endl;
                        e.getInfo().append(result);
                        errmsg = "Failed to send migrate commit to configs because " + e.getInfo().toString();

                        return false;
                    }
                    else {
                        // same as if we catch any other exception
                        error() << "moveChunk failed to get confirmation of commit" << migrateLog;
                        error() << "TERMINATING" << migrateLog;
                        dumpCrashInfo("moveChunk failed to get confirmation of commit");
                        dbexit(EXIT_SHARDING_ERROR);
                    }
                }
                catch (...) {
                    error() << "moveChunk failed to get confirmation of commit" << migrateLog;
                    error() << "TERMINATING" << migrateLog;
                    dumpCrashInfo("moveChunk failed to get confirmation of commit");
                    dbexit(EXIT_SHARDING_ERROR);
                }

                migrateFromStatus.setInCriticalSection( false );

                // 5.d
                configServer.logChange( "moveChunk.commit" , ns , chunkInfo );
            }

            migrateFromStatus.done(false);
            timing.done(5);

            // 6.
            // Vanilla MongoDB checks for cursors in the chunk, and if any exist, it starts a background thread that waits for those cursors to leave before doing the delete.
            // We have MVCC so we don't need to wait, we can just do the delete.
            cleanupOldData(ns, shardKeyPattern, min, max);
            timing.done(6);
            return true;
        }

    } moveChunkCmd;

    bool ShardingState::inCriticalMigrateSection() {
        return migrateFromStatus.getInCriticalSection();
    }

    bool ShardingState::waitTillNotInCriticalSection( int maxSecondsToWait ) {
        return migrateFromStatus.waitTillNotInCriticalSection( maxSecondsToWait );
    }

    /* -----
       below this are the "to" side commands

       command to initiate
       worker thread
         does initial clone
         pulls initial change set
         keeps pulling
         keeps state
       command to get state
       commend to "commit"
    */

    class MigrateStatus {
        long long _lastAppliedMigrateLogID;

        template<typename T>
        class CounterResetter : boost::noncopyable {
            T &_val;
            T _was;
            bool _done;
          public:
            CounterResetter(T &val) : _val(val), _was(val), _done(false) {}
            ~CounterResetter() {
                if (!_done) {
                    _val = _was;
                }
            }
            void setDone() { _done = true; }
        };

    public:
        
        MigrateStatus() : _lastAppliedMigrateLogID(-1), m_active("MigrateStatus") { active = false; }

        void prepare(const BSONObj &cmdObj) {
            scoped_lock l(m_active); // reading and writing 'active'

            verify( ! active );
            state = READY;
            errmsg = "";

            numCloned = 0;
            clonedBytes = 0;
            numCatchup = 0;
            numSteady = 0;
            _lastAppliedMigrateLogID = -1;

            active = true;

            ns = cmdObj.firstElement().String();
            from = cmdObj["from"].String();
            min = cmdObj["min"].Obj().getOwned();
            max = cmdObj["max"].Obj().getOwned();
            if (cmdObj.hasField("shardKeyPattern")) {
                shardKeyPattern = cmdObj["shardKeyPattern"].Obj().getOwned();
            } else {
                // TODO: can we remove this section since TokuMX clusters shouldn't have any pre-2.2
                // protocol artifacts?

                // shardKeyPattern may not be provided if another shard is from pre 2.2
                // In that case, assume the shard key pattern is the same as the range
                // specifiers provided.
                BSONObj keya = KeyPattern::inferKeyPattern(min);
                BSONObj keyb = KeyPattern::inferKeyPattern(max);
                verify( keya == keyb );

                warning() << "No shard key pattern provided by source shard for migration."
                    " This is likely because the source shard is running a version prior to 2.2."
                    " Falling back to assuming the shard key matches the pattern of the min and max"
                    " chunk range specifiers.  Inferred shard key: " << keya << endl;

                shardKeyPattern = keya.getOwned();
            }
        }

        /**
         * transferMods uses conn to pull operations out of the donor's migratelog and apply them
         * locally on the recipient.
         *
         * There is some fanciness with the locking to avoid holding the read lock during a remote
         * round-trip.  Note that SpillableVectorIterator will perform round-trips when you call
         * more(), so we peek at the object even though we shouldn't, to see if it's a real
         * reference or not.
         *
         * The alternative would be to never hold the ReadContext for longer than a single
         * SpillableVectorIterator's lifetime, but this is also bad because in the common case, each
         * op will have just one op so we want to be able to hold the lock and transaction for a
         * whole batch in the outer cursor.
         *
         * So, each time we see an op with an "a" field, we start a ReadContext and Transaction if
         * one wasn't open yet (in the hopes that we'll see more ops with "a" fields and be able to
         * zip through the current batch with them), and each time we see an op that needs to dive
         * into the refs collection, we commit and kill the current Transaction and ReadContext so
         * that we can start one for each batch of the SpillableVectorIterator.
         */
        GTID transferMods(ScopedDbConnection &conn) {
            auto_ptr<DBClientCursor> mlogCursor(conn->query(MigrateFromStatus::MIGRATE_LOG_NS,
                                                            QUERY("_id" << GTE << _lastAppliedMigrateLogID).hint(BSON("_id" << 1)),
                                                            0, 0, 0, 0));
            massert(17320, "migratelog cursor query returned NULL, aborting migration", mlogCursor.get() != NULL);
            // Check that what we thought was the last one really is the last one and we don't have a gap.
            if (_lastAppliedMigrateLogID >= 0) {
                BSONObj op = mlogCursor->nextSafe();
                verify(op["_id"].Long() == _lastAppliedMigrateLogID);
            }
            while (mlogCursor->more()) {
                LOCK_REASON(lockReason, "sharding: applying mods for migrate");
                try {
                    scoped_ptr<Client::ReadContext> ctxp(new Client::ReadContext(ns, lockReason));
                    scoped_ptr<Client::Transaction> txnp(new Client::Transaction(DB_SERIALIZABLE));
                    scoped_ptr<DBClientCursor::BatchResetter> br(new DBClientCursor::BatchResetter(*mlogCursor));
                    scoped_ptr<CounterResetter<long long> > mlidResetter(new CounterResetter<long long>(_lastAppliedMigrateLogID));
                    scoped_ptr<CounterResetter<long long> > catchupResetter(new CounterResetter<long long>(numCatchup));
                    scoped_ptr<CounterResetter<long long> > steadyResetter(new CounterResetter<long long>(numSteady));
                    while (mlogCursor->moreInCurrentBatch()) {
                        BSONObj op = mlogCursor->nextSafe();
                        dassert(op["_id"].Long() == _lastAppliedMigrateLogID + 1);
                        if (op.hasField("a")) {
                            if (!txnp) {
                                ctxp.reset(new Client::ReadContext(ns, lockReason));
                                txnp.reset(new Client::Transaction(DB_SERIALIZABLE));
                                br.reset(new DBClientCursor::BatchResetter(*mlogCursor));
                                mlidResetter.reset(new CounterResetter<long long>(_lastAppliedMigrateLogID));
                                catchupResetter.reset(new CounterResetter<long long>(numCatchup));
                                steadyResetter.reset(new CounterResetter<long long>(numSteady));
                            }
                            SpillableVectorIterator it(op, conn.conn(), MigrateFromStatus::MIGRATE_LOG_REF_NS);
                            while (it.more()) {
                                OplogHelpers::applyOperationFromOplog(it.next());
                                if (state == CATCHUP) {
                                    numCatchup++;
                                } else {
                                    numSteady++;
                                }
                            }
                        } else {
                            if (txnp) {
                                txnp->commit();
                                br->setDone();
                                mlidResetter->setDone();
                                catchupResetter->setDone();
                                steadyResetter->setDone();

                                txnp.reset();
                                ctxp.reset();
                                br.reset();
                                mlidResetter.reset();
                                catchupResetter.reset();
                                steadyResetter.reset();
                            }
                            SpillableVectorIterator it(op, conn.conn(), MigrateFromStatus::MIGRATE_LOG_REF_NS);
                            while (it.more()) {
                                LOCK_REASON(lockReasonInner, "sharding: applying mods from migratelog.refs for migrate");
                                try {
                                    Client::ReadContext ctx(ns, lockReasonInner);
                                    Client::Transaction txn(DB_SERIALIZABLE);
                                    SpillableVectorIterator::BatchResetter itbr(it);
                                    CounterResetter<long long> cr(numCatchup);
                                    CounterResetter<long long> sr(numSteady);
                                    while (it.moreInCurrentBatch()) {
                                        OplogHelpers::applyOperationFromOplog(it.next());
                                        if (state == CATCHUP) {
                                            numCatchup++;
                                        } else {
                                            numSteady++;
                                        }
                                    }
                                    txn.commit();
                                    itbr.setDone();
                                    cr.setDone();
                                    sr.setDone();
                                } catch (RetryWithWriteLock) {
                                    Client::WriteContext ctx(ns, lockReasonInner);
                                    Client::Transaction txn(DB_SERIALIZABLE);
                                    while (it.moreInCurrentBatch()) {
                                        OplogHelpers::applyOperationFromOplog(it.next());
                                        if (state == CATCHUP) {
                                            numCatchup++;
                                        } else {
                                            numSteady++;
                                        }
                                    }
                                    txn.commit();
                                }
                            }
                        }
                        _lastAppliedMigrateLogID = op["_id"].Long();
                    }
                    if (txnp) {
                        txnp->commit();
                        br->setDone();
                        mlidResetter->setDone();
                        catchupResetter->setDone();
                        steadyResetter->setDone();
                    }
                } catch (RetryWithWriteLock) {
                    // This case is much simpler because we hopefully only have a normal op, not a
                    // spilled op (if that got RetryWithWriteLock, it should have been handled in
                    // the inner try/catch block).  We know we won't yield for this entire batch, so
                    // we just blast right through it.
                    //
                    // WARNING: Normally, code like this would be factored out.  If something
                    // changes in the above try block, something should probably change here too.
                    Client::WriteContext ctx(ns, lockReason);
                    Client::Transaction txn(DB_SERIALIZABLE);
                    int opsHandled = 0;
                    while (mlogCursor->moreInCurrentBatch()) {
                        BSONObj op = mlogCursor->nextSafe();
                        dassert(op["_id"].Long() == _lastAppliedMigrateLogID + 1);
                        if (!op.hasField("a")) {
                            if (opsHandled == 0) {
                                warning() << "moveChunk: transferMods found a spilled migratelog operation in a write lock,"
                                          << " but hasn't processed anything properly yet." << migrateLog;
                                std::stringstream ss;
                                ss << "Aborting migration to prevent infinite loop in transferMods due to op " << op;
                                warning() << ss.str() << migrateLog;
                                msgasserted(17327, ss.str());
                            }
                            LOG(0) << "moveChunk: transferMods found a spilled migratelog operation in a write lock,"
                                   << " stopping here and returning to normal transferMods implementation." << migrateLog;
                            mlogCursor->putBack(op);
                            break;
                        }
                        SpillableVectorIterator it(op, conn.conn(), MigrateFromStatus::MIGRATE_LOG_REF_NS);
                        while (it.more()) {
                            OplogHelpers::applyOperationFromOplog(it.next());
                            if (state == CATCHUP) {
                                numCatchup++;
                            } else {
                                numSteady++;
                            }
                        }
                        _lastAppliedMigrateLogID = op["_id"].Long();
                        opsHandled++;
                    }
                    txn.commit();
                }
                GTID lastGTID = cc().getLastOp();
                for (Timer t; !opReplicatedEnough(lastGTID); sleepmillis(20)) {
                    if (t.seconds() > 60) {
                        RARELY warning() << "secondaries are having a hard time keeping up with migration" << migrateLog;
                        if (t.seconds() > 600) {
                            break;
                        }
                    }
                }
            }
            return cc().getLastOp();
        }

        /**
         * We may need to handle RetryWithWriteLock inside this code, so it is factored out of _go
         * below.
         */
        void lockedMigrateInsertFirstBatch(const BSONObj &firstBatch, uint64_t insertFlags) {
            Client::Transaction txn(DB_SERIALIZABLE);
            Collection *cl = getCollection(ns);
            massert(17318, "collection must exist during migration", cl);
            for (BSONObjIterator it(firstBatch); it.more(); ++it) {
                BSONObj obj = (*it).Obj();
                insertOneObject(cl, obj, insertFlags);
                OplogHelpers::logInsert(ns.c_str(), obj, true);
                numCloned++;
                clonedBytes += obj.objsize();
            }
            txn.commit();
        }

        void lockedMigrateInsertBatch(DBClientCursorBatchIterator &iter, uint64_t insertFlags) {
            Client::Transaction txn(DB_SERIALIZABLE);
            Collection *cl = getCollection(ns);
            massert(17319, "collection must exist during migration", cl);
            while (iter.moreInCurrentBatch()) {
                BSONObj obj = iter.nextSafe();
                insertOneObject(cl, obj, insertFlags);
                OplogHelpers::logInsert(ns.c_str(), obj, true);
                numCloned++;
                clonedBytes += obj.objsize();
            }
            txn.commit();
        }

        bool lockedMigrateHandleLegacyBatch(const BSONObj &arr) {
            Client::Transaction txn(DB_SERIALIZABLE);
            int thisTime = 0;

            for (BSONObjIterator i(arr); i.more(); i++) {
                BSONObj o = (*i).Obj();
                BSONObj id = o["_id"].wrap();
                OpDebug debug;
                updateObjects(ns.c_str(),
                              o,
                              id,
                              true,  // upsert
                              false, // multi
                              true   // fromMigrate
                              );

                thisTime++;
                numCloned++;
                clonedBytes += o.objsize();
            }

            txn.commit();

            return thisTime != 0;
        }

        void go() {
            try {
                _go();
            }
            catch ( std::exception& e ) {
                state = FAIL;
                errmsg = e.what();
                error() << "migrate failed: " << e.what() << migrateLog;
            }
            catch ( ... ) {
                state = FAIL;
                errmsg = "UNKNOWN ERROR";
                error() << "migrate failed with unknown exception" << migrateLog;
            }
            setActive( false );
        }

        void _go() {
            verify( getActive() );
            verify( state == READY );
            verify( ! min.isEmpty() );
            verify( ! max.isEmpty() );
            
            replSetMajorityCount = theReplSet ? theReplSet->config().getMajority() : 0;

            log() << "starting receiving-end of migration of chunk " << min << " -> " << max <<
                    " for collection " << ns << " from " << from << endl;

            string errmsg;
            MoveTimingHelper timing( "to" , ns , min , max , 5 /* steps */ , errmsg );

            scoped_ptr<ScopedDbConnection> connPtr(
                    ScopedDbConnection::getScopedDbConnection( from ) );
            ScopedDbConnection& conn = *connPtr;
            conn->getLastError(); // just test connection

            {
                // 0. copy system.namespaces entry if collection doesn't already exist
                vector<BSONObj> indexes;
                for (auto_ptr<DBClientCursor> indexCursor = conn->getIndexes(ns); indexCursor->more(); ) {
                    indexes.push_back(indexCursor->next().getOwned());
                }

                bool needCreate;
                {
                    LOCK_REASON(lockReason, "sharding: checking collection and indexes for migrate");
                    Client::ReadContext ctx(ns, lockReason);
                    Client::Transaction txn(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);

                    Collection *cl = getCollection(ns);
                    needCreate = cl == NULL;

                    if (!needCreate) {
                        for (vector<BSONObj>::const_iterator it = indexes.begin(); it != indexes.end(); ++it) {
                            const BSONObj &idx = *it;
                            if (cl->findIndexByKeyPattern(idx.getObjectField("key")) < 0) {
                                warning() << "During migrate for collection " << ns << ", noticed that we are missing an index " << idx << "." << migrateLog;
                                warning() << "You should schedule some time soon to build this index on this shard, or drop it elsewhere." << migrateLog;
                            }
                        }
                    }

                    txn.commit();
                }

                if (needCreate) {
                    string system_namespaces = getSisterNS(ns, "system.namespaces");
                    BSONObj entry = conn->findOne(system_namespaces, BSON( "name" << ns ));

                    LOCK_REASON(lockReason, "sharding: creating collection for migrate");
                    Client::WriteContext ctx(ns, lockReason);
                    Client::Transaction txn(DB_SERIALIZABLE);
                    // Now that we have the write lock, check that the collection still doesn't
                    // exist, otherwise it may be expensive to build the right indexes.
                    Collection *cl = getCollection(ns);
                    if (cl == NULL) {
                        BSONObj opts = entry.getObjectField("options");
                        if (!opts.isEmpty()) {
                            string errmsg;
                            if (!userCreateNS(ns, opts, errmsg, true)) {
                                warning() << "failed to create collection " << ns << " with options " << opts << ": " << errmsg << migrateLog;
                            }
                        }
                        string system_indexes = getSisterNS(ns, "system.indexes");
                        for (vector<BSONObj>::const_iterator it = indexes.begin(); it != indexes.end(); ++it) {
                            const BSONObj &idxObj = *it;
                            insertObject(system_indexes.c_str(), idxObj, 0, true /* flag fromMigrate in oplog */);
                        }
                    } else {
                        LOG(0) << "During migrate for collection " << ns << ", it didn't exist but after getting the write lock, it now exists." << migrateLog;
                        LOG(0) << "This could be a race, shouldn't happen, but is benign as long as the indexes are correct." << migrateLog;
                        bool anyMissing = false;
                        for (vector<BSONObj>::const_iterator it = indexes.begin(); it != indexes.end(); ++it) {
                            const BSONObj &idx = *it;
                            if (cl->findIndexByKeyPattern(idx.getObjectField("key")) < 0) {
                                anyMissing = true;
                                warning() << "The index " << idx << " seems to be missing." << migrateLog;
                                warning() << "We'll proceed with the migration without that index for now, but you should build that index or drop it elsewhere soon." << migrateLog;
                            }
                        }
                        if (!anyMissing) {
                            LOG(0) << "All indexes are present." << migrateLog;
                        }
                    }
                    txn.commit();
                }
                timing.done(1);
            }

            {
                // 2. delete any data already in range
                LOCK_REASON(lockReason, "sharding: deleting old documents before migrate");
                Client::ReadContext ctx(ns, lockReason);

                BSONObj indexKeyPattern;
                if ( !findShardKeyIndexPattern_locked( ns, shardKeyPattern, &indexKeyPattern ) ) {
                    errmsg = "collection doesn't have the shard key indexed during migrate";
                    warning() << errmsg << endl;
                    state = FAIL;
                    return;
                }

                Client::Transaction txn(DB_SERIALIZABLE);
                long long num = deleteIndexRange( ns, min, max, indexKeyPattern,
                                                  false, /*maxInclusive*/
                                                  true ); /* flag fromMigrate in oplog */
                txn.commit();

                if ( num )
                    warning() << "moveChunkCmd deleted data already in chunk # objects: " << num << migrateLog;

                timing.done(2);
            }


            {
                // 3. initial bulk clone
                state = CLONE;

                BSONObj res;

                if (!conn->runCommand("admin", BSON("listCommands" << 1), res)) {
                    state = FAIL;
                    errmsg = mongoutils::str::stream() << "listCommands failed: " << res.toString();
                    error() << errmsg << migrateLog;
                    conn.done();
                    return;
                }
                BSONObj cmds = res["commands"].Obj();
                bool hasNewCloneCommands = cmds.hasField("_migrateStartCloneTransaction");

                if (hasNewCloneCommands) {
                    if (!conn->runCommand("admin", BSON("_migrateStartCloneTransaction" << 1 <<
                                                        "ns" << ns <<
                                                        "keyPattern" << shardKeyPattern <<
                                                        "min" << min <<
                                                        "max" << max), res)) {
                        state = FAIL;
                        errmsg = mongoutils::str::stream() << "_migrateStartCloneTransaction failed: " << res.toString();
                        error() << errmsg << migrateLog;
                        conn.done();
                        return;
                    }

                    BSONObj cursorObj = res["cursor"].Obj();
                    massert(17225, mongoutils::str::stream() << "expected cursor ns " << ns << ", got " << cursorObj["ns"].Stringdata(),
                            cursorObj["ns"].Stringdata() == ns);

                    uint64_t insertFlags = Collection::NO_LOCKTREE;
                    if (!migrateUniqueChecks) {
                        insertFlags |= Collection::NO_UNIQUE_CHECKS;
                    }

                    LOCK_REASON(lockReason, "sharding: cloning documents on recipient for migrate");
                    try {
                        Client::ReadContext ctx(ns, lockReason);
                        CounterResetter<long long> numClonedResetter(numCloned);
                        CounterResetter<long long> clonedBytesResetter(clonedBytes);

                        lockedMigrateInsertFirstBatch(cursorObj["firstBatch"].Obj(), insertFlags);

                        numClonedResetter.setDone();
                        clonedBytesResetter.setDone();
                    } catch (RetryWithWriteLock) {
                        Client::WriteContext ctx(ns, lockReason);

                        lockedMigrateInsertFirstBatch(cursorObj["firstBatch"].Obj(), insertFlags);
                    }

                    for (DBClientCursor cursor(conn.get(), ns, cursorObj["id"].Long(), 0, 0);
                         cursor.more(); ) {
                        try {
                            Client::ReadContext ctx(ns, lockReason);
                            CounterResetter<long long> numClonedResetter(numCloned);
                            CounterResetter<long long> clonedBytesResetter(clonedBytes);
                            DBClientCursor::BatchResetter br(cursor);
                            DBClientCursorBatchIterator iter(cursor);

                            lockedMigrateInsertBatch(iter, insertFlags);

                            numClonedResetter.setDone();
                            clonedBytesResetter.setDone();
                            br.setDone();
                        } catch (RetryWithWriteLock) {
                            Client::WriteContext ctx(ns, lockReason);
                            DBClientCursorBatchIterator iter(cursor);

                            lockedMigrateInsertBatch(iter, insertFlags);
                        }
                    }
                } else {
                    // The old path, for compatibility with older TokuMX servers.
                    LOG(0) << "moveChunk using old migrate path, please upgrade all shards soon" << migrateLog;

                    while ( true ) {
                        BSONObj res;
                        if ( ! conn->runCommand( "admin" , BSON( "_migrateClone" << 1 ) , res ) ) {  // gets array of objects to copy, in disk order
                            state = FAIL;
                            errmsg = "_migrateClone failed: ";
                            errmsg += res.toString();
                            error() << errmsg << migrateLog;
                            conn.done();
                            return;
                        }

                        BSONObj arr = res["objects"].Obj();
                        bool nonempty;
                        LOCK_REASON(lockReason, "sharding: cloning documents on recipient for migrate, using old _migrateClone");
                        try {
                            Client::ReadContext ctx(ns, lockReason);
                            CounterResetter<long long> numClonedResetter(numCloned);
                            CounterResetter<long long> clonedBytesResetter(clonedBytes);

                            nonempty = lockedMigrateHandleLegacyBatch(arr);

                            numClonedResetter.setDone();
                            clonedBytesResetter.setDone();
                        } catch (RetryWithWriteLock) {
                            Client::WriteContext ctx(ns, lockReason);

                            nonempty = lockedMigrateHandleLegacyBatch(arr);
                        }
                        if (!nonempty) {
                            break;
                        }
                    }
                }

                timing.done(3);
            }

            // if running on a replicated system, we'll need to flush the docs we cloned to the secondaries
            GTID lastGTID = cc().getLastOp();

            {
                // 4. do bulk of mods
                state = CATCHUP;

                lastGTID = transferMods(conn);

                const int maxIterations = 60*60*50;
                int i;
                for (i = 0; i < maxIterations; ++i) {
                    if (state == ABORT) {
                        timing.note("aborted");
                        return;
                    }

                    if (opReplicatedEnough(lastGTID)) {
                        break;
                    }

                    if (i > 100) {
                        warning() << "secondaries having hard time keeping up with migrate" << migrateLog;
                    }

                    sleepmillis(20);
                }

                if ( i == maxIterations ) {
                    errmsg = "secondary can't keep up with migrate";
                    error() << errmsg << migrateLog;
                    conn.done();
                    state = FAIL;
                    return;
                }

                timing.done(4);
            }

            {
                // pause to wait for replication
                // this will prevent us from going into critical section until we're ready
                for (Timer t; t.minutes() < 600; sleepsecs(1)) {
                    log() << "Waiting for replication to catch up before entering critical section"
                          << endl;
                    if (opReplicatedEnough(lastGTID)) {
                        break;
                    }
                }
            }

            {
                // 5. wait for commit

                state = STEADY;
                bool transferAfterCommit = false;
                while ( state == STEADY || state == COMMIT_START ) {

                    // Make sure we do at least one transfer after recv'ing the commit message
                    // If we aren't sure that at least one transfer happens *after* our state
                    // changes to COMMIT_START, there could be mods still on the FROM shard that
                    // got logged *after* our _transferMods but *before* the critical section.
                    if ( state == COMMIT_START ) transferAfterCommit = true;

                    lastGTID = transferMods(conn);

                    if ( state == ABORT ) {
                        timing.note( "aborted" );
                        return;
                    }

                    // We know we're finished when:
                    // 1) The from side has told us that it has locked writes (COMMIT_START)
                    // 2) We've checked at least one more time for un-transmitted mods
                    if ( state == COMMIT_START && transferAfterCommit == true ) {
                        if (opReplicatedEnough(lastGTID)) {
                            storage::log_flush();
                            break;
                        }
                    }

                    // Only sleep if we aren't committing
                    if ( state == STEADY ) sleepmillis( 10 );
                }

                if ( state == FAIL ) {
                    errmsg = "timed out waiting for commit";
                    return;
                }

                timing.done(5);
            }

            state = DONE;
            conn.done();
        }

        void status( BSONObjBuilder& b ) {
            b.appendBool( "active" , getActive() );

            b.append( "ns" , ns );
            b.append( "from" , from );
            b.append( "min" , min );
            b.append( "max" , max );
            b.append( "shardKeyPattern" , shardKeyPattern );

            b.append( "state" , stateString() );
            if ( state == FAIL )
                b.append( "errmsg" , errmsg );
            {
                BSONObjBuilder bb( b.subobjStart( "counts" ) );
                bb.append( "cloned" , numCloned );
                bb.append( "clonedBytes" , clonedBytes );
                bb.append( "catchup" , numCatchup );
                bb.append( "steady" , numSteady );
                bb.done();
            }


        }

        bool apply(const vector<BSONElement> &modElements, GTID* lastGTID) {
            if (modElements.empty()) {
                return false;
            }

            GTID dummy;
            if ( lastGTID == NULL ) {
                lastGTID = &dummy;
            }

            LOCK_REASON(lockReason, "sharding: applying mods for migrate, using old _transferMods");
            Client::ReadContext ctx(ns, lockReason);
            Client::Transaction txn(DB_SERIALIZABLE);

            for (vector<BSONElement>::const_iterator it = modElements.begin(); it != modElements.end(); ++it) {
                BSONObj mod = it->Obj();
                vector<BSONElement> logObjElts = mod["a"].Array();
                for (vector<BSONElement>::const_iterator lit = logObjElts.begin(); lit != logObjElts.end(); ++lit) {
                    OplogHelpers::applyOperationFromOplog(lit->Obj());
                }
            }

            txn.commit();
            *lastGTID = ctx.ctx().getClient()->getLastOp();

            return true;
        }

        bool opReplicatedEnough( const GTID& lastGTID ) {
            // if replication is on, try to force enough secondaries to catch up
            // TODO opReplicatedEnough should eventually honor priorities and geo-awareness
            //      for now, we try to replicate to a sensible number of secondaries
            return mongo::opReplicatedEnough( lastGTID , replSetMajorityCount );
        }

        string stateString() {
            switch ( state ) {
            case READY: return "ready";
            case CLONE: return "clone";
            case CATCHUP: return "catchup";
            case STEADY: return "steady";
            case COMMIT_START: return "commitStart";
            case DONE: return "done";
            case FAIL: return "fail";
            case ABORT: return "abort";
            }
            verify(0);
            return "";
        }

        bool startCommit() {
            if ( state != STEADY )
                return false;
            state = COMMIT_START;
            
            Timer t;
            // we wait for the commit to succeed before giving up
            // This timeout is longer than for vanilla mongodb since we may have a large transactions in the queue for transferMods.
            for (int i = 0; t.seconds() <= 300; ++i) {
                log() << "Waiting for commit to finish" << endl;
                sleepmillis( 1 << std::min( i , 10 ) );
                if (state == FAIL) {
                    return false;
                }
                if (state == DONE) {
                    return true;
                }
            }
            state = FAIL;
            log() << "startCommit never finished!" << migrateLog;
            return false;
        }

        void abort() {
            state = ABORT;
            errmsg = "aborted";
        }

        bool getActive() const { scoped_lock l(m_active); return active; }
        void setActive( bool b ) { scoped_lock l(m_active); active = b; }

        mutable mongo::mutex m_active;
        bool active;

        string ns;
        string from;

        BSONObj min;
        BSONObj max;
        BSONObj shardKeyPattern;

        long long numCloned;
        long long clonedBytes;
        long long numCatchup;
        long long numSteady;

        int replSetMajorityCount;

        enum State { READY , CLONE , CATCHUP , STEADY , COMMIT_START , DONE , FAIL , ABORT } state;
        string errmsg;

    } migrateStatus;

    void migrateThread() {
        Client::initThread( "migrateThread" );
        if (!noauth) {
            ShardedConnectionInfo::addHook();
            cc().getAuthorizationManager()->grantInternalAuthorization("_migrateThread");
        }
        migrateStatus.go();
        cc().shutdown();
    }

    class RecvChunkStartCommand : public ChunkCommandHelper {
    public:
        RecvChunkStartCommand() : ChunkCommandHelper( "_recvChunkStart" ) {}

        virtual LockType locktype() const { return OPLOCK; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_recvChunkStart);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {

            if ( migrateStatus.getActive() ) {
                errmsg = "migrate already in progress";
                return false;
            }
            
            if ( ! configServer.ok() )
                ShardingState::initialize(cmdObj["configServer"].String());

            migrateStatus.prepare(cmdObj);

            boost::thread m( migrateThread );

            result.appendBool( "started" , true );
            return true;
        }

    } recvChunkStartCmd;

    class RecvChunkStatusCommand : public ChunkCommandHelper {
    public:
        RecvChunkStatusCommand() : ChunkCommandHelper( "_recvChunkStatus" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_recvChunkStatus);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            migrateStatus.status( result );
            return 1;
        }

    } recvChunkStatusCommand;

    class RecvChunkCommitCommand : public ChunkCommandHelper {
    public:
        RecvChunkCommitCommand() : ChunkCommandHelper( "_recvChunkCommit" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_recvChunkCommit);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            bool ok = migrateStatus.startCommit();
            migrateStatus.status( result );
            return ok;
        }

    } recvChunkCommitCommand;

    class RecvChunkAbortCommand : public ChunkCommandHelper {
    public:
        RecvChunkAbortCommand() : ChunkCommandHelper( "_recvChunkAbort" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_recvChunkAbort);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            migrateStatus.abort();
            migrateStatus.status( result );
            return true;
        }

    } recvChunkAbortCommand;


    class IsInRangeTest : public StartupTest {
    public:
        void run() {
            BSONObj min = BSON( "x" << 1 );
            BSONObj max = BSON( "x" << 5 );
            BSONObj skey = BSON( "x" << 1 );

            verify( ! isInRange( BSON( "x" << 0 ) , min , max , skey ) );
            verify( isInRange( BSON( "x" << 1 ) , min , max , skey ) );
            verify( isInRange( BSON( "x" << 3 ) , min , max , skey ) );
            verify( isInRange( BSON( "x" << 4 ) , min , max , skey ) );
            verify( ! isInRange( BSON( "x" << 5 ) , min , max , skey ) );
            verify( ! isInRange( BSON( "x" << 6 ) , min , max , skey ) );

            BSONObj obj = BSON( "n" << 3 );
            BSONObj min2 = BSON( "x" << BSONElementHasher::hash64( obj.firstElement() , 0 ) - 2 );
            BSONObj max2 = BSON( "x" << BSONElementHasher::hash64( obj.firstElement() , 0 ) + 2 );
            BSONObj hashedKey =  BSON( "x" << "hashed" );

            verify( isInRange( BSON( "x" << 3 ) , min2 , max2 , hashedKey ) );
            verify( ! isInRange( BSON( "x" << 3 ) , min , max , hashedKey ) );
            verify( ! isInRange( BSON( "x" << 4 ) , min2 , max2 , hashedKey ) );

            LOG(1) << "isInRangeTest passed" << migrateLog;
        }
    } isInRangeTest;
}
