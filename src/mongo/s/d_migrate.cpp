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
#include <map>
#include <string>
#include <vector>

#include <boost/thread/thread.hpp>

#include "mongo/pch.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/database.h"
#include "mongo/db/dbhelpers.h"
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
#include "mongo/db/repl/rs.h"
#include "mongo/db/txn_context.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/ops/update.h"

#include "mongo/client/connpool.h"
#include "mongo/client/distlock.h"
#include "mongo/client/dbclientcursor.h"
#include "mongo/client/remote_transaction.h"

#include "mongo/util/queue.h"
#include "mongo/util/startup_test.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/ramlog.h"
#include "mongo/util/elapsed_tracker.h"

#include "mongo/s/shard.h"
#include "mongo/s/d_logic.h"
#include "mongo/s/config.h"
#include "mongo/s/chunk.h"

using namespace std;

namespace mongo {

    BSONObj findShardKeyIndexPattern_locked( const string& ns , const BSONObj& shardKeyPattern ) {
        verify( Lock::isLocked() );
        NamespaceDetails* nsd = nsdetails( ns.c_str() );
        verify( nsd );
        const IndexDetails* idx = nsd->findIndexByPrefix( shardKeyPattern , true );  /* require single key */
        verify( idx );
        return idx->keyPattern().getOwned();
    }


    BSONObj findShardKeyIndexPattern_unlocked( const string& ns , const BSONObj& shardKeyPattern ) {
        Client::ReadContext context( ns );
        return findShardKeyIndexPattern_locked( ns , shardKeyPattern ).getOwned();
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

    struct OldDataCleanup {
        static AtomicUInt _numThreads; // how many threads are doing async cleanup

        string ns;
        BSONObj min;
        BSONObj max;
        BSONObj shardKeyPattern;
        set<CursorId> initial;

        OldDataCleanup(){
            _numThreads++;
        }
        OldDataCleanup( const OldDataCleanup& other ) {
            ns = other.ns;
            min = other.min.getOwned();
            max = other.max.getOwned();
            shardKeyPattern = other.shardKeyPattern.getOwned();
            initial = other.initial;
            _numThreads++;
        }
        ~OldDataCleanup(){
            _numThreads--;
        }

        string toString() const {
            return str::stream() << ns << " from " << min << " -> " << max;
        }
        
        void doRemove() {
            ShardForceVersionOkModeBlock sf;
            {
                // TODO: TokuMX: No need for a removesaver, we have transactions.
#if 0
                RemoveSaver rs("moveChunk",ns,"post-cleanup");
#endif

                log() << "moveChunk starting delete for: " << this->toString() << migrateLog;

                long long numDeleted =
                        Helpers::removeRange( ns ,
                                              min ,
                                              max ,
                                              findShardKeyIndexPattern_unlocked( ns , shardKeyPattern ) , 
                                              false , /*maxInclusive*/
                                              /* cmdLine.moveParanoia ? &rs : 0 , */ /*callback*/
                                              true ); /*fromMigrate*/

                log() << "moveChunk deleted " << numDeleted << " documents for "
                      << this->toString() << migrateLog;
            }

            GTID lastGTID = cc().getLastOp();
            Timer t;
            for ( int i=0; i<3600; i++ ) {
                if ( opReplicatedEnough( lastGTID, ( getSlaveCount() / 2 ) + 1 ) ) {
                    LOG(t.seconds() < 30 ? 1 : 0) << "moveChunk repl sync took " << t.seconds() << " seconds" << migrateLog;
                    return;
                }
                sleepsecs(1);
            }

            warning() << "moveChunk repl sync timed out after " << t.seconds() << " seconds" << migrateLog;
        }

    };

    AtomicUInt OldDataCleanup::_numThreads = 0;

    static const char * const cleanUpThreadName = "cleanupOldData";

    class ChunkCommandHelper : public Command {
    public:
        ChunkCommandHelper( const char * name )
            : Command( name ) {
        }

        virtual void help( stringstream& help ) const {
            help << "internal - should not be called directly";
        }
        virtual bool slaveOk() const { return false; }
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
                : _m("MigrateFromStatus"),
                  _inCriticalSection(false),
                  _active(false),
                  _memoryUsed(0),
                  _workLock("MigrateFromStatus::workLock"),
                  _migrateLogDetails(NULL),
                  _migrateLogRefDetails(NULL),
                  _nextMigrateLogId(0),
                  _snapshotTaken(false) {}

        void start( const std::string& ns ,
                    const BSONObj& min ,
                    const BSONObj& max ,
                    const BSONObj& shardKeyPattern ) {

            //
            // Do not hold _workLock
            //

            //scoped_lock ll(_workLock);

            scoped_lock l(_m); // reads and writes _active

            verify( ! _active );

            verify( ! min.isEmpty() );
            verify( ! max.isEmpty() );
            verify( ns.size() );

            _ns = ns;
            _min = min;
            _max = max;
            _shardKeyPattern = shardKeyPattern;

            verify( _clonePKs.size() == 0 );
            verify( _deleted.size() == 0 );
            verify( _reload.size() == 0 );
            verify( _memoryUsed == 0 );

            _snapshotTaken = false;
            clearMigrateLog();
            _active = true;
        }

        void done() {
            log() << "MigrateFromStatus::done About to acquire global write lock to exit critical "
                    "section" << endl;
            Lock::GlobalWrite lk;
            log() << "MigrateFromStatus::done Global lock acquired" << endl;

            {
                scoped_spinlock lk( _trackerLocks );
                _deleted.clear();
                _reload.clear();
                _clonePKs.clear();
            }
            _memoryUsed = 0;

            scoped_lock l(_m);
            disableLogTxnOpsForSharding();
            _snapshotTaken = false;
            clearMigrateLog();
            _active = false;
            _inCriticalSection = false;
        }

        void clearMigrateLog() {
            string err;
            BSONObjBuilder res;
            try {
                Client::WriteContext ctx(MIGRATE_LOG_NS);
                Client::Transaction txn(DB_SERIALIZABLE);
                dropCollection(MIGRATE_LOG_NS, err, res, false);
                _nextMigrateLogId.store(0);
                _nextIdToTransfer = 0;
                _nextRefSeqToTransfer = 0;
                _migrateLogDetails = getAndMaybeCreateNS(MIGRATE_LOG_NS, false);
                verify(_migrateLogDetails != NULL);
                txn.commit();
            }
            catch (DBException &e) {
                stringstream ss;
                ss << "Error clearing " << MIGRATE_LOG_NS << " to prepare for chunk migration."
                   << " err: " << err
                   << " res: " << res.obj()
                   << " exc: " << e.what();
                problem() << ss.str() << endl;
                throw e;
            }
            try {
                Client::WriteContext ctx(MIGRATE_LOG_REF_NS);
                Client::Transaction txn(DB_SERIALIZABLE);
                dropCollection(MIGRATE_LOG_REF_NS, err, res, false);
                _migrateLogRefDetails = getAndMaybeCreateNS(MIGRATE_LOG_REF_NS, false);
                verify(_migrateLogRefDetails != NULL);
                txn.commit();
            }
            catch (DBException &e) {
                stringstream ss;
                ss << "Error clearing " << MIGRATE_LOG_REF_NS << " to prepare for chunk migration."
                   << " err: " << err
                   << " res: " << res.obj()
                   << " exc: " << e.what();
                problem() << ss.str() << endl;
                throw e;
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
                    !(mongoutils::str::equals(opstr, OpLogHelpers::OP_STR_CAPPED_INSERT) ||
                      mongoutils::str::equals(opstr, OpLogHelpers::OP_STR_CAPPED_DELETE)));

            if (!_snapshotTaken) {
                return false;
            }

            if (mongoutils::str::equals(opstr, OpLogHelpers::OP_STR_DELETE) &&
                getThreadName().find(cleanUpThreadName) == 0) {
                // This really shouldn't happen but I'm having a hard time proving it right now.
                problem() << "Someone tried to log a delete for migration while we're cleaning up a migration."
                          << " This doesn't make sense since those deletes should be marked fromMigrate."
                          << " The op is"
                          << " opstr: " << opstr
                          << " ns: " << ns
                          << " obj: " << obj << endl;
                // we don't want to xfer things we're cleaning
                // as then they'll be deleted on TO
                // which is bad
                return false;
            }

            if (mongoutils::str::equals(opstr, OpLogHelpers::OP_STR_INSERT) ||
                mongoutils::str::equals(opstr, OpLogHelpers::OP_STR_DELETE) ||
                mongoutils::str::equals(opstr, OpLogHelpers::OP_STR_UPDATE)) {
                return isInRange(obj, _min, _max, _shardKeyPattern);
            }
            return false;
        }

        bool shouldLogUpdateOp(const char *opstr, const char *ns, const BSONObj &oldObj, const BSONObj &newObj) {
            // Just a sanity check.  ShardStrategy::_prepareUpdate() appears to prevent you from updating a document such that the shard key changes.
            // We just verify that old and new have the same shard key, and pass to the normal case.
            // But we call shouldLogOp first to avoid doing the comparison if, say, we're in the wrong ns and we can stop early.
            bool should = shouldLogOp(opstr, ns, oldObj);
            if (should) {
                ShardKeyPattern shardKey(_shardKeyPattern);
                BSONObj oldKey = shardKey.extractKey(oldObj);
                BSONObj newKey = shardKey.extractKey(newObj);
                verify(oldKey.equal(newKey));
            }
            return should;
        }

        void startObjForMigrateLog(BSONObjBuilder &b) {
            b << "_id" << _nextMigrateLogId.fetchAndAdd(1);
        }

        void writeObjToMigrateLog(BSONObj &obj) {
            Client::ReadContext ctx(MIGRATE_LOG_NS);
            insertOneObject(_migrateLogDetails, obj,
                            NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE);
        }

        void writeObjToMigrateLogRef(BSONObj &obj) {
            Client::ReadContext ctx(MIGRATE_LOG_REF_NS);
            insertOneObject(_migrateLogRefDetails, obj,
                            NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE);
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

            NamespaceDetails *d;
            if (_cc.get() == NULL) {
                dassert(!_txn);
                Client::WriteContext ctx(_ns);
                enableLogTxnOpsForSharding(mongo::shouldLogOpForSharding,
                                           mongo::shouldLogUpdateOpForSharding,
                                           mongo::startObjForMigrateLog,
                                           mongo::writeObjToMigrateLog,
                                           mongo::writeObjToMigrateLogRef);
                _snapshotTaken = true;
                _txn.reset(new Client::Transaction(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY));

                d = nsdetails(_ns.c_str());
                if (d == NULL) {
                    errmsg = "ns not found, should be impossible";
                    _txn.reset();
                    return false;
                }

                const IndexDetails *idx = d->findIndexByPrefix( _shardKeyPattern ,
                                                                true );  /* require single key */

                if ( idx == NULL ) {
                    errmsg = mongoutils::str::stream() << "can't find index for " << _shardKeyPattern << " in _migrateClone" << causedBy( errmsg );
                    _txn.reset();
                    return false;
                }
                // Assume both min and max non-empty, append MinKey's to make them fit chosen index
                BSONObj min = Helpers::modifiedRangeBound( _min , idx->keyPattern() , -1 );
                BSONObj max = Helpers::modifiedRangeBound( _max , idx->keyPattern() , -1 );

                shared_ptr<Cursor> idxCursor(IndexCursor::make( d , *idx , min , max , false , 1 ));
                _cc.reset(new ClientCursor(QueryOption_NoCursorTimeout, idxCursor, _ns));

                cc().swapTransactionStack(_txnStack);
            }

            {
                Client::WithTxnStack wts(_txnStack);
                Client::ReadContext ctx(_ns);

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

        long long mbUsed() const { return _memoryUsed / ( 1024 * 1024 ); }

        bool getInCriticalSection() const { scoped_lock l(_m); return _inCriticalSection; }
        void setInCriticalSection( bool b ) { scoped_lock l(_m); _inCriticalSection = b; }

        bool isActive() const { return _getActive(); }
        
        void doRemove( OldDataCleanup& cleanup ) {
            for (int it = 0; true; it++) {
                if ( it > 20 && it % 10 == 0 ) {
                    log() << "doRemote iteration " << it << " for: " << cleanup << endl;
                }
                scoped_lock ll(_workLock);
                if ( ! _active ) {
                    Client::ReadContext ctx(cleanup.ns);
                    Client::Transaction txn(DB_SERIALIZABLE);
                    cleanup.doRemove();
                    txn.commit();
                    return;
                }
                sleepmillis( 1000 );
            }
        }

    private:
        mutable mongo::mutex _m; // protect _inCriticalSection and _active
        bool _inCriticalSection;
        bool _active;

        string _ns;
        BSONObj _min;
        BSONObj _max;
        BSONObj _shardKeyPattern;

        // we need the lock in case there is a malicious _migrateClone for example
        // even though it shouldn't be needed under normal operation
        SpinLock _trackerLocks;

        // primary keys yet to be transferred from here to the other side
        // no locking needed because built initially by 1 thread in a read lock
        // emptied by 1 thread in a read lock
        // updates applied by 1 thread in a write lock
        set<BSONObj> _clonePKs;

        list<BSONObj> _reload; // objects that were modified that must be recloned
        list<BSONObj> _deleted; // objects deleted during clone that should be deleted later
        long long _memoryUsed; // bytes in _reload + _deleted

        mutable mongo::mutex _workLock; // this is used to make sure only 1 thread is doing serious work
                                        // for now, this means migrate or removing old chunk data

        static const char MIGRATE_LOG_NS[];
        static const char MIGRATE_LOG_REF_NS[];
        NamespaceDetails *_migrateLogDetails;
        NamespaceDetails *_migrateLogRefDetails;
        AtomicWord<long long> _nextMigrateLogId;
        long long _nextIdToTransfer;
        long long _nextRefSeqToTransfer;

        bool _snapshotTaken;
        scoped_ptr<Client::Transaction> _txn;
        shared_ptr<Client::TransactionStack> _txnStack;
        auto_ptr<ClientCursor> _cc;

        bool _getActive() const { scoped_lock l(_m); return _active; }
        void _setActive( bool b ) { scoped_lock l(_m); _active = b; }

    } migrateFromStatus;

    const char MigrateFromStatus::MIGRATE_LOG_NS[] = "local.migratelog.sh";
    const char MigrateFromStatus::MIGRATE_LOG_REF_NS[] = "local.migratelogref.sh";

    struct MigrateStatusHolder {
        MigrateStatusHolder( const std::string& ns ,
                             const BSONObj& min ,
                             const BSONObj& max ,
                             const BSONObj& shardKeyPattern ) {
            migrateFromStatus.start( ns , min , max , shardKeyPattern );
        }
        ~MigrateStatusHolder() {
            migrateFromStatus.done();
        }
    };

    void _cleanupOldData( OldDataCleanup cleanup ) {

        Client::initThread((string(cleanUpThreadName) + string("-") +
                                                        OID::gen().toString()).c_str());

        if (!noauth) {
            cc().getAuthorizationManager()->grantInternalAuthorization("_cleanupOldData");
        }

        log() << " (start) waiting to cleanup " << cleanup
              << ", # cursors remaining: " << cleanup.initial.size() << migrateLog;

        int loops = 0;
        Timer t;
        while ( t.seconds() < 900 ) { // 15 minutes
            verify( !Lock::isLocked() );
            sleepmillis( 20 );

            set<CursorId> now;
            ClientCursor::find( cleanup.ns , now );

            set<CursorId> left;
            for ( set<CursorId>::iterator i=cleanup.initial.begin(); i!=cleanup.initial.end(); ++i ) {
                CursorId id = *i;
                if ( now.count(id) )
                    left.insert( id );
            }

            if ( left.size() == 0 )
                break;
            cleanup.initial = left;

            if ( ( loops++ % 200 ) == 0 ) {
                log() << " (looping " << loops << ") waiting to cleanup " << cleanup.ns << " from " << cleanup.min << " -> " << cleanup.max << "  # cursors:" << cleanup.initial.size() << migrateLog;

                stringstream ss;
                for ( set<CursorId>::iterator i=cleanup.initial.begin(); i!=cleanup.initial.end(); ++i ) {
                    CursorId id = *i;
                    ss << id << " ";
                }
                log() << " cursors: " << ss.str() << migrateLog;
            }
        }

        migrateFromStatus.doRemove( cleanup );

        cc().shutdown();
    }

    void cleanupOldData( OldDataCleanup cleanup ) {
        try {
            _cleanupOldData( cleanup );
        }
        catch ( std::exception& e ) {
            log() << " error cleaning old data:" << e.what() << migrateLog;
        }
        catch ( ... ) {
            log() << " unknown error cleaning old data" << migrateLog;
        }
    }

    bool shouldLogOpForSharding(const char *opstr, const char *ns, const BSONObj &obj) {
        return migrateFromStatus.shouldLogOp(opstr, ns, obj);
    }

    bool shouldLogUpdateOpForSharding(const char *opstr, const char *ns, const BSONObj &oldObj, const BSONObj &newObj) {
        return migrateFromStatus.shouldLogUpdateOp(opstr, ns, oldObj, newObj);
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
                dlk = dist_lock_try( &lockSetup , (string)"migrate-" + min.toString() );
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

            ShardChunkVersion maxVersion;
            ShardChunkVersion startingVersion;
            string myOldShard;
            {
                scoped_ptr<ScopedDbConnection> conn(
                        ScopedDbConnection::getInternalScopedDbConnection(
                                shardingState.getConfigServer() ) );

                BSONObj x;
                BSONObj currChunk;
                try{
                    x = conn->get()->findOne( ShardNS::chunk,
                                              Query( BSON( "ns" << ns ) )
                                                  .sort( BSON( "lastmod" << -1 ) ) );
                    currChunk = conn->get()->findOne( ShardNS::chunk , shardId.wrap( "_id" ) );
                }
                catch( DBException& e ){
                    errmsg = str::stream() << "aborted moveChunk because could not get chunk data from config server " << shardingState.getConfigServer() << causedBy( e );
                    warning() << errmsg << endl;
                    return false;
                }

                maxVersion = ShardChunkVersion::fromBSON( x, "lastmod" );
                verify( currChunk["shard"].type() );
                verify( currChunk["min"].type() );
                verify( currChunk["max"].type() );
                myOldShard = currChunk["shard"].String();
                conn->done();

                BSONObj currMin = currChunk["min"].Obj();
                BSONObj currMax = currChunk["max"].Obj();
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

                LOG(0) << "moveChunk data transfer progress: " << res << " my mem used: " << migrateFromStatus.mbUsed() << migrateLog;

                if ( ! ok || res["state"].String() == "fail" ) {
                    warning() << "moveChunk error transferring data caused migration abort: " << res << migrateLog;
                    errmsg = "data transfer error";
                    result.append( "cause" , res );
                    return false;
                }

                if ( res["state"].String() == "steady" )
                    break;

                if ( migrateFromStatus.mbUsed() > (500 * 1024 * 1024) ) {
                    // this is too much memory for us to use for this
                    // so we're going to abort the migrate
                    scoped_ptr<ScopedDbConnection> conn(
                            ScopedDbConnection::getScopedDbConnection( toShard.getConnString() ) );

                    BSONObj res;
                    conn->get()->runCommand( "admin" , BSON( "_recvChunkAbort" << 1 ) , res );
                    res = res.getOwned();
                    conn->done();
                    error() << "aborting migrate because too much memory used res: " << res << migrateLog;
                    errmsg = "aborting migrate because too much memory used";
                    result.appendBool( "split" , true );
                    return false;
                }

                killCurrentOp.checkForInterrupt();
            }
            timing.done(4);

            // 5.
            {
                // 5.a
                // we're under the collection lock here, so no other migrate can change maxVersion or ShardChunkManager state
                migrateFromStatus.setInCriticalSection( true );
                ShardChunkVersion myVersion = maxVersion;
                myVersion.incMajor();

                {
                    // TODO(leif): Why is this lock needed? Try to remove this lock or downgrade to a read lock later.
                    Lock::DBWrite lk( ns );
                    verify( myVersion > shardingState.getVersion( ns ) );

                    // bump the chunks manager's version up and "forget" about the chunk being moved
                    // this is not the commit point but in practice the state in this shard won't until the commit it done
                    shardingState.donateChunk( ns , min , max , myVersion );
                }

                log() << "moveChunk setting version to: " << myVersion << migrateLog;

                // 5.b
                // we're under the collection lock here, too, so we can undo the chunk donation because no other state change
                // could be ongoing
                {
                    BSONObj res;
                    // This timeout (330 seconds) is bigger than on vanilla mongodb, since the
                    // transferMods we have to do even though we think we're in a steady state could
                    // be much larger than in vanilla.
                    scoped_ptr<ScopedDbConnection> connTo(
                            ScopedDbConnection::getScopedDbConnection( toShard.getConnString(),
                                                                       330.0 ) );

                    bool ok;

                    try{
                        ok = connTo->get()->runCommand( "admin" ,
                                                        BSON( "_recvChunkCommit" << 1 ) ,
                                                        res );
                    }
                    catch( DBException& e ){
                        errmsg = str::stream() << "moveChunk could not contact to: shard " << toShard.getConnString() << " to commit transfer" << causedBy( e );
                        warning() << errmsg << endl;
                        ok = false;
                    }

                    connTo->done();

                    if ( ! ok ) {
                        log() << "moveChunk migrate commit not accepted by TO-shard: " << res
                              << " resetting shard version to: " << startingVersion << migrateLog;
                        {
                            // TODO(leif): Why is this a global lock while the lock above at donateChunk is a db-level lock?
                            Lock::GlobalWrite lk;
                            log() << "moveChunk global lock acquired to reset shard version from "
                                    "failed migration" << endl;

                            // revert the chunk manager back to the state before "forgetting" about the chunk
                            shardingState.undoDonateChunk( ns , min , max , startingVersion );
                        }
                        log() << "Shard version successfully reset to clean up failed migration"
                                << endl;

                        errmsg = "_recvChunkCommit failed!";
                        result.append( "cause" , res );
                        return false;
                    }

                    log() << "moveChunk migrate commit accepted by TO-shard: " << res << migrateLog;
                }

                // 5.c

                // version at which the next highest lastmod will be set
                // if the chunk being moved is the last in the shard, nextVersion is that chunk's lastmod
                // otherwise the highest version is from the chunk being bumped on the FROM-shard
                ShardChunkVersion nextVersion;

                // we want to go only once to the configDB but perhaps change two chunks, the one being migrated and another
                // local one (so to bump version for the entire shard)

                try {
                    scoped_ptr<ScopedDbConnection> conn(ScopedDbConnection::getInternalScopedDbConnection(shardingState.getConfigServer(), 10.0));
                    RemoteTransaction txn(conn->conn(), "serializable");

                    // Check the precondition
                    BSONObjBuilder b;
                    b.appendTimestamp("lastmod", maxVersion.toLong());
                    BSONObj expect = b.obj();
                    Matcher m(expect);

                    BSONObj found = conn->get()->findOne(ShardNS::chunk, QUERY("ns" << ns).sort("lastmod", -1));
                    if (!m.matches(found)) {
                        // TODO(leif): Make sure that this means the sharding algorithm is broken and we should bounce the server.
                        error() << "moveChunk commit failed: " << ShardChunkVersion::fromBSON(found["lastmod"])
                                << " instead of " << maxVersion << migrateLog;
                        error() << "TERMINATING" << migrateLog;
                        dbexit(EXIT_SHARDING_ERROR);
                    }

                    try {
                        // update for the chunk being moved
                        BSONObjBuilder n;
                        n.append( "_id" , Chunk::genID( ns , min ) );
                        myVersion.addToBSON( n, "lastmod" );
                        n.append( "ns" , ns );
                        n.append( "min" , min );
                        n.append( "max" , max );
                        n.append( "shard" , toShard.getName() );
                        conn->get()->update(ShardNS::chunk, QUERY("_id" << Chunk::genID(ns, min)), n.obj());
                    }
                    catch (DBException &e) {
                        warning() << e << migrateLog;
                        error() << "moveChunk error updating the chunk being moved" << migrateLog;
                        throw e;
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
                            n.append( "_id" , Chunk::genID( ns , bumpMin ) );
                            nextVersion.addToBSON( n, "lastmod" );
                            n.append( "ns" , ns );
                            n.append( "min" , bumpMin );
                            n.append( "max" , bumpMax );
                            n.append( "shard" , fromShard.getName() );
                            conn->get()->update(ShardNS::chunk, QUERY("_id" << Chunk::genID(ns, bumpMin)), n.obj());
                            log() << "moveChunk updating self version to: " << nextVersion << " through "
                                  << bumpMin << " -> " << bumpMax << " for collection '" << ns << "'" << migrateLog;
                        }
                        catch (DBException &e) {
                            warning() << e << migrateLog;
                            error() << "moveChunk error updating chunk on the FROM shard" << migrateLog;
                            throw e;
                        }
                    }
                    else {
                        log() << "moveChunk moved last chunk out for collection '" << ns << "'" << migrateLog;
                    }

                    txn.commit();
                    conn->done();
                }
                catch (...) {
                    // TODO(leif): Vanilla, if it fails, waits 10 seconds and does a query to see if somehow the commit made it through anyway.  Maybe we need such a mechanism too?
                    error() << "moveChunk failed to get confirmation of commit" << migrateLog;
                    error() << "TERMINATING" << migrateLog;
                    dbexit(EXIT_SHARDING_ERROR);
                }

                // Vanilla does the following at the end.  Keep this code around as notes until we know we don't need it.
#if 0
                if ( ! ok ) {

                    // this could be a blip in the connectivity
                    // wait out a few seconds and check if the commit request made it
                    //
                    // if the commit made it to the config, we'll see the chunk in the new shard and there's no action
                    // if the commit did not make it, currently the only way to fix this state is to bounce the mongod so
                    // that the old state (before migrating) be brought in

                    warning() << "moveChunk commit outcome ongoing: " << cmd << " for command :" << cmdResult << migrateLog;
                    sleepsecs( 10 );

                    try {
                        scoped_ptr<ScopedDbConnection> conn(
                                ScopedDbConnection::getInternalScopedDbConnection(
                                        shardingState.getConfigServer(),
                                        10.0 ) );

                        // look for the chunk in this shard whose version got bumped
                        // we assume that if that mod made it to the config, the applyOps was successful
                        BSONObj doc = conn->get()->findOne( ShardNS::chunk,
                                                            Query(BSON( "ns" << ns ))
                                                                .sort( BSON("lastmod" << -1)));
                        ShardChunkVersion checkVersion = ShardChunkVersion::fromBSON( doc["lastmod"] );

                        if ( checkVersion.isEquivalentTo( nextVersion ) ) {
                            log() << "moveChunk commit confirmed" << migrateLog;

                        }
                        else {
                            error() << "moveChunk commit failed: version is at"
                                            << checkVersion << " instead of " << nextVersion << migrateLog;
                            error() << "TERMINATING" << migrateLog;
                            dbexit( EXIT_SHARDING_ERROR );
                        }

                        conn->done();

                    }
                    catch ( ... ) {
                        error() << "moveChunk failed to get confirmation of commit" << migrateLog;
                        error() << "TERMINATING" << migrateLog;
                        dbexit( EXIT_SHARDING_ERROR );
                    }
                }
#endif

                migrateFromStatus.setInCriticalSection( false );

                // 5.d
                configServer.logChange( "moveChunk.commit" , ns , chunkInfo );
            }

            migrateFromStatus.done();
            timing.done(5);

            {
                // 6.
                OldDataCleanup c;
                c.ns = ns;
                c.min = min.getOwned();
                c.max = max.getOwned();
                c.shardKeyPattern = shardKeyPattern.getOwned();
                // Vanilla MongoDB checks for cursors in the chunk, and if any exist, it starts a background thread that waits for those cursors to leave before doing the delete.
                // We have MVCC so we don't need to wait, we can just do the delete.
                // TODO: get rid of the OldDataCleanup class and just do the removeRange right here.
                c.doRemove();
            }
            timing.done(6);

            return true;

        }

    } moveChunkCmd;

    bool ShardingState::inCriticalMigrateSection() {
        return migrateFromStatus.getInCriticalSection();
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
    public:
        
        MigrateStatus() : m_active("MigrateStatus") { active = false; }

        void prepare() {
            scoped_lock l(m_active); // reading and writing 'active'

            verify( ! active );
            state = READY;
            errmsg = "";

            numCloned = 0;
            clonedBytes = 0;
            numCatchup = 0;
            numSteady = 0;

            active = true;
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
                Client::WriteContext ctx( ns );
                Client::Transaction txn(DB_SERIALIZABLE);
                const string &dbname = cc().database()->name();

                // Only copy if ns doesn't already exist
                if ( ! nsdetails( ns.c_str() ) ) {
                    string system_namespaces = dbname + ".system.namespaces";
                    BSONObj entry = conn->findOne( system_namespaces, BSON( "name" << ns ) );
                    if ( entry["options"].isABSONObj() ) {
                        string errmsg;
                        if ( ! userCreateNS( ns.c_str(), entry["options"].Obj(), errmsg, true ) )
                            warning() << "failed to create collection with options: " << errmsg
                                      << endl;
                    }
                }

                // 1. copy indexes

                {
                    auto_ptr<DBClientCursor> indexes = conn->getIndexes( ns );
                    string system_indexes = dbname + ".system.indexes";
                    while ( indexes->more() ) {
                        BSONObj idx = indexes->next();
                        insertObject( system_indexes.c_str() , idx, 0, true /* flag fromMigrate in oplog */ );
                    }
                }

                txn.commit();
                timing.done(1);
            }

            {
                // 2. delete any data already in range
                // removeRange makes a ReadContext and a Transaction
                long long num = Helpers::removeRange( ns ,
                                                      min ,
                                                      max ,
                                                      findShardKeyIndexPattern_unlocked( ns , shardKeyPattern ) , 
                                                      false , /*maxInclusive*/
                                                      true ); /* flag fromMigrate in oplog */
                if ( num )
                    warning() << "moveChunkCmd deleted data already in chunk # objects: " << num << migrateLog;

                timing.done(2);
            }


            {
                // 3. initial bulk clone
                state = CLONE;

                Client::ReadContext ctx(ns);
                Client::Transaction txn(DB_SERIALIZABLE);

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
                    int thisTime = 0;

                    BSONObjIterator i( arr );
                    while( i.more() ) {
                        BSONObj o = i.next().Obj();
                        BSONObj id = o["_id"].wrap();
                        OpDebug debug;
                        updateObjects(ns.c_str(),
                                      o,
                                      id,
                                      true,  // upsert
                                      false, // multi
                                      true,  // logop
                                      debug,
                                      true   // fromMigrate
                                      );

                        thisTime++;
                        numCloned++;
                        clonedBytes += o.objsize();
                    }

                    if ( thisTime == 0 )
                        break;
                }

                txn.commit();
                timing.done(3);
            }

            // if running on a replicated system, we'll need to flush the docs we cloned to the secondaries
            GTID lastGTID = cc().getLastOp();

            {
                // 4. do bulk of mods
                state = CATCHUP;
                while ( true ) {
                    BSONObj res;
                    if ( ! conn->runCommand( "admin" , BSON( "_transferMods" << 1 ) , res ) ) {
                        state = FAIL;
                        errmsg = "_transferMods failed: ";
                        errmsg += res.toString();
                        error() << "_transferMods failed: " << res << migrateLog;
                        conn.done();
                        return;
                    }
                    vector<BSONElement> modElements = res["mods"].Array();
                    if (modElements.empty()) {
                        break;
                    }

                    apply(modElements, &lastGTID);

                    const int maxIterations = 3600*50;
                    int i;
                    for ( i=0;i<maxIterations; i++) {
                        if ( state == ABORT ) {
                            timing.note( "aborted" );
                            return;
                        }

                        if ( opReplicatedEnough( lastGTID ) )
                            break;

                        if ( i > 100 ) {
                            warning() << "secondaries having hard time keeping up with migrate" << migrateLog;
                        }

                        sleepmillis( 20 );
                    }

                    if ( i == maxIterations ) {
                        errmsg = "secondary can't keep up with migrate";
                        error() << errmsg << migrateLog;
                        conn.done();
                        state = FAIL;
                        return;
                    }
                }

                timing.done(4);
            }

            {
                // pause to wait for replication
                // this will prevent us from going into critical section until we're ready
                Timer t;
                while ( t.minutes() < 600 ) {
                    log() << "Waiting for replication to catch up before entering critical section"
                          << endl;
                    if ( flushPendingWrites( lastGTID ) )
                        break;
                    sleepsecs(1);
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

                    BSONObj res;
                    if ( ! conn->runCommand( "admin" , BSON( "_transferMods" << 1 ) , res ) ) {
                        log() << "_transferMods failed in STEADY state: " << res << migrateLog;
                        errmsg = res.toString();
                        state = FAIL;
                        conn.done();
                        return;
                    }

                    vector<BSONElement> modElements = res["mods"].Array();
                    if (apply(modElements, &lastGTID)) {
                        continue;
                    }

                    if ( state == ABORT ) {
                        timing.note( "aborted" );
                        return;
                    }

                    // We know we're finished when:
                    // 1) The from side has told us that it has locked writes (COMMIT_START)
                    // 2) We've checked at least one more time for un-transmitted mods
                    if ( state == COMMIT_START && transferAfterCommit == true ) {
                        if ( flushPendingWrites( lastGTID ) )
                            break;
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

            Client::ReadContext ctx(ns);
            Client::Transaction txn(DB_SERIALIZABLE);

            for (vector<BSONElement>::const_iterator it = modElements.begin(); it != modElements.end(); ++it) {
                BSONObj mod = it->Obj();
                vector<BSONElement> logObjElts = mod["a"].Array();
                for (vector<BSONElement>::const_iterator lit = logObjElts.begin(); lit != logObjElts.end(); ++lit) {
                    OpLogHelpers::applyOperationFromOplog(lit->Obj());
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

        bool flushPendingWrites( const GTID& lastGTID ) {
            if ( ! opReplicatedEnough( lastGTID ) ) {
                OCCASIONALLY warning() << "migrate commit waiting for " << replSetMajorityCount 
                                       << " slaves for '" << ns << "' " << min << " -> " << max 
                                       << " waiting for: " << lastGTID.toString()
                                       << migrateLog;
                return false;
            }

            log() << "migrate commit succeeded flushing to secondaries for '" << ns << "' " << min << " -> " << max << migrateLog;

            {
                Lock::GlobalRead lk;

                // if durability is on, force a write to journal
#if 0
                if ( getDur().commitNow() ) {
                    log() << "migrate commit flushed to journal for '" << ns << "' " << min << " -> " << max << migrateLog;
                }
#endif
                // TODO: TokuMX What do we have to do here?
            }

            return true;
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
                if ( state == DONE )
                    return true;
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

        virtual LockType locktype() const { return WRITE; }  // this is so don't have to do locking internally
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_recvChunkStart);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {

            if ( migrateStatus.getActive() ) {
                errmsg = "migrate already in progress";
                return false;
            }
            
            if ( OldDataCleanup::_numThreads > 0 ) {
                errmsg = 
                    str::stream() 
                    << "still waiting for a previous migrates data to get cleaned, can't accept new chunks, num threads: " 
                    << OldDataCleanup::_numThreads;
                return false;
            }

            if ( ! configServer.ok() )
                ShardingState::initialize(cmdObj["configServer"].String());

            migrateStatus.prepare();

            migrateStatus.ns = cmdObj.firstElement().String();
            migrateStatus.from = cmdObj["from"].String();
            migrateStatus.min = cmdObj["min"].Obj().getOwned();
            migrateStatus.max = cmdObj["max"].Obj().getOwned();
            if (cmdObj.hasField("shardKeyPattern")) {
                migrateStatus.shardKeyPattern = cmdObj["shardKeyPattern"].Obj().getOwned();
            } else {
                // shardKeyPattern may not be provided if another shard is from pre 2.2
                // In that case, assume the shard key pattern is the same as the range
                // specifiers provided.
                BSONObj keya , keyb;
                Helpers::toKeyFormat( migrateStatus.min , keya );
                Helpers::toKeyFormat( migrateStatus.max , keyb );
                verify( keya == keyb );

                warning() << "No shard key pattern provided by source shard for migration."
                    " This is likely because the source shard is running a version prior to 2.2."
                    " Falling back to assuming the shard key matches the pattern of the min and max"
                    " chunk range specifiers.  Inferred shard key: " << keya << endl;

                migrateStatus.shardKeyPattern = keya.getOwned();
            }

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
