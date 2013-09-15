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

#include <algorithm>
#include <list>
#include <map>
#include <vector>
#include <utility>

#include <boost/filesystem/operations.hpp>

#include "mongo/db/cursor.h"
#include "mongo/db/database.h"
#include "mongo/db/databaseholder.h"
#include "mongo/db/json.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/oplog.h"
#include "mongo/db/relock.h"
#include "mongo/db/txn_context.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/txn.h"
#include "mongo/db/storage/key.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/scripting/engine.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/repl/rs_optime.h"
#include "mongo/db/repl/rs.h"

namespace mongo {

    NamespaceIndex *nsindex(const StringData& ns) {
        Database *database = cc().database();
        verify( database );
        DEV {
            StringData db = nsToDatabaseSubstring(ns);
            if ( db != database->name() ) {
                out() << "ERROR: attempt to write to wrong database\n";
                out() << " ns:" << ns << '\n';
                out() << " database->name:" << database->name() << endl;
                verify( db == database->name() );
            }
        }
        return &database->_nsIndex;
    }

    NamespaceDetails *nsdetails(const StringData& ns) {
        return nsindex(ns)->details(ns);
    }

    NamespaceDetails *nsdetails_maybe_create(const StringData& ns, BSONObj options) {
        NamespaceIndex *ni = nsindex(ns);
        if (!ni->allocated()) {
            // Must make sure we loaded any existing namespaces before checking, or we might create one that already exists.
            ni->init(true);
        }
        NamespaceDetails *details = ni->details(ns);
        if (details == NULL) {
            TOKULOG(2) << "Didn't find nsdetails(" << ns << "), creating it." << endl;
            if (!Lock::isWriteLocked(ns)) {
                throw RetryWithWriteLock();
            }

            shared_ptr<NamespaceDetails> new_details( NamespaceDetails::make(ns, options) );
            ni->add_ns(ns, new_details);

            details = ni->details(ns);
            details->addDefaultIndexesToCatalog();

            // Keep the call to 'str()', it allows us to call it in gdb.
            TOKULOG(2) << "Created nsdetails " << options.str() << endl;
        }
        return details;
    }

#pragma pack(1)
    struct IDToInsert {
        IDToInsert() : type((char) jstOID) {
            memcpy(_id, "_id", sizeof(_id));
            dassert( sizeof(IDToInsert) == 17 );
            oid.init();
        }
        const char type;
        char _id[4];
        OID oid;
    };
#pragma pack()

    BSONObj addIdField(const BSONObj &obj) {
        if (obj.hasField("_id")) {
            return obj;
        } else {
            IDToInsert id;
            BSONObjBuilder b;
            // _id first, everything else after
            b.append(BSONElement(reinterpret_cast<const char *>(&id)));
            b.appendElements(obj);
            return b.obj();
        }
    }

    BSONObj inheritIdField(const BSONObj &oldObj, const BSONObj &newObj) {
        const BSONElement &e = newObj["_id"];
        if (e.ok()) {
            uassert( 13596 ,
                     str::stream() << "cannot change _id of a document old:" << oldObj << " new:" << newObj,
                     e.valuesEqual(oldObj["_id"]) );
            return newObj;
        } else {
            BSONObjBuilder b;
            b.append(oldObj["_id"]);
            b.appendElements(newObj);
            return b.obj();
        }
    }

    class IndexedCollection : public NamespaceDetails {
    public:
        IndexedCollection(const StringData &ns, const BSONObj &options) :
            NamespaceDetails(ns, fromjson("{\"_id\":1}"), options) {
        }
        IndexedCollection(const BSONObj &serialized) :
            NamespaceDetails(serialized) {
        }

        // regular collections are eligble for queryIdHack(), see ops/query.cpp
        bool mayFindById() const {
            dassert(hasIdIndex());
            return true;
        }

        // finds an objectl by _id field, which in the case of indexed collections
        // is the primary key.
        bool findById(const BSONObj &query, BSONObj &result) const {
            dassert(query["_id"].ok());
            return findByPK(query["_id"].wrap(""), result);
        }

        // inserts an object into this namespace, taking care of secondary indexes if they exist
        void insertObject(BSONObj &obj, uint64_t flags) {
            obj = addIdField(obj);
            BSONObj pk = obj["_id"].wrap(""); // TODO: .wrap() is a malloc/copy, let's try not to do that.
            insertIntoIndexes(pk, obj, flags);
        }

        void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj, uint64_t flags) {
            newObj = inheritIdField(oldObj, newObj);
            NamespaceDetails::updateObject(pk, oldObj, newObj, flags);
        }
    };

    class OplogCollection : public IndexedCollection {
    public:
        OplogCollection(const StringData &ns, const BSONObj &options) :
            IndexedCollection(ns, options) {
        } 
        // Important: BulkLoadedCollection relies on this constructor
        // doing nothing more than calling the parent IndexedCollection
        // constructor. If this constructor ever does more, we need to
        // modify BulkLoadedCollection to match behavior for the oplog.
        OplogCollection(const BSONObj &serialized) :
            IndexedCollection(serialized) {
        }
        // @return the maximum safe key to read for a tailable cursor.
        BSONObj minUnsafeKey() {
            if (theReplSet && theReplSet->gtidManager) {
                BSONObjBuilder b;
                GTID minUncommitted = theReplSet->gtidManager->getMinLiveGTID();
                addGTIDToBSON("", minUncommitted, b);
                return b.obj();
            }
            else {
                return minKey;
            }
        }
        void hotOptimizeOplog(GTID end) {
            // do a hot optimize up until gtid;
            BSONObjBuilder q;
            addGTIDToBSON("", end, q);
            IndexDetails &pkIdx = getPKIndex();
            BSONObj done = q.done();
            pkIdx.hotOptimizeRange(NULL, NULL, &done, NULL);
        }
    };

    class NaturalOrderCollection : public NamespaceDetails {
    public:
        NaturalOrderCollection(const StringData &ns, const BSONObj &options) :
            NamespaceDetails(ns, fromjson("{\"$_\":1}"), options),
            _nextPK(0) {
        }
        NaturalOrderCollection(const BSONObj &serialized) :
            NamespaceDetails(serialized),
            _nextPK(0) {

            // the next PK, if it exists, is the last key + 1
            IndexDetails &pkIdx = getPKIndex();
            Client::Transaction txn(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
            {
                IndexDetails::Cursor c(pkIdx);
                DBC *cursor = c.dbc();

                BSONObj key = BSONObj();
                struct getfLastExtra extra(key);
                const int r = cursor->c_getf_last(cursor, 0, getfLastCallback, &extra);
                if (extra.ex != NULL) {
                    throw *extra.ex;
                }
                if (r != 0 && r != DB_NOTFOUND) {
                    storage::handle_ydb_error(r);
                }
                if (!key.isEmpty()) {
                    dassert(key.nFields() == 1);
                    _nextPK = AtomicWord<long long>(key.firstElement().Long() + 1);
                }
            }
            txn.commit();
        }

        // insert an object, using a fresh auto-increment primary key
        void insertObject(BSONObj &obj, uint64_t flags) {
            BSONObjBuilder pk(64);
            pk.append("", _nextPK.fetchAndAdd(1));
            insertIntoIndexes(pk.obj(), obj, flags);
        }

    protected:
        AtomicWord<long long> _nextPK;

    private:
        struct getfLastExtra {
            BSONObj &key;
            std::exception *ex;
            getfLastExtra(BSONObj &k) : key(k), ex(NULL) { }
        };

        static int getfLastCallback(const DBT *key, const DBT *value, void *extra) {
            struct getfLastExtra *info = reinterpret_cast<struct getfLastExtra *>(extra);
            try {
                if (key != NULL) {
                    const storage::Key sKey(key);
                    info->key = sKey.key().getOwned();
                }
                return 0;
            } catch (std::exception &e) {
                info->ex = &e;
            }
            return -1;
        }
    };

    class SystemCatalogCollection : public NaturalOrderCollection {
    public:
        SystemCatalogCollection(const StringData &ns, const BSONObj &options) :
            NaturalOrderCollection(ns, options) {
        }
        SystemCatalogCollection(const BSONObj &serialized) :
            NaturalOrderCollection(serialized) {
        }

        // strip out the _id field before inserting into a system collection
        void insertObject(BSONObj &obj, uint64_t flags) {
            obj = beautify(obj);
            BSONObj result;
            const BSONElement &e = obj["ns"];
            DEV {
                // This findOne call will end up creating a BasicCursor over
                // system.indexes which will prelock the collection. That
                // prevents concurrent threads from doing file-ops, which is
                // bad in production.
                const bool exists = findOne(e.ok() ? BSON("name" << obj["name"] << "ns" << e) :
                                                     BSON("name" << obj["name"]), result);
                massert( 16891, str::stream() << "bug: system catalog tried to insert " <<
                                obj << ", but something similar already exists: " << result,
                                !exists);
            }
            NaturalOrderCollection::insertObject(obj, flags);
        }

    private:
        void createIndex(const BSONObj &info) {
            msgasserted(16464, "bug: system collections should not be indexed." );
        }

        // For consistency with Vanilla MongoDB, the system catalogs have the following
        // fields, in order, if they exist.
        //
        //  { key, unique, ns, name, [everything else] }
        //
        // This code is largely borrowed from prepareToBuildIndex() in Vanilla.
        BSONObj beautify(const BSONObj &obj) {
            BSONObjBuilder b;
            if (obj["key"].ok()) {
                b.append(obj["key"]);
            }
            if (obj["unique"].trueValue()) {
                b.appendBool("unique", true);
            }
            if (obj["ns"].ok()) {
                b.append(obj["ns"]);
            }
            if (obj["name"].ok()) { 
                b.append(obj["name"]);
            }
            for (BSONObjIterator i = obj.begin(); i.more(); ) {
                BSONElement e = i.next();
                string s = e.fieldName();
                if (s != "key" && s != "unique" && s != "ns" && s != "name" && s != "_id") {
                    b.append(e);
                }
            }
            return b.obj();
        }
    };

    // Capped collections have natural order insert semantics but borrow (ie: copy)
    // its document modification strategy from IndexedCollections. The size
    // and count of a capped collection is maintained in memory and kept valid
    // on txn abort through a CappedCollectionRollback class in the TxnContext. 
    //
    // Tailable cursors over capped collections may only read up to one less
    // than the minimum uncommitted primary key to ensure that they never miss
    // any data. This information is communicated through minUnsafeKey(). On
    // commit/abort, the any primary keys inserted into a capped collection are
    // noted so we can properly maintain the min uncommitted key.
    //
    // In the implementation, NaturalOrderCollection::_nextPK and the set of
    // uncommitted primary keys are protected together by _mutex. Trimming
    // work is done under the _deleteMutex.
    class CappedCollection : public NaturalOrderCollection {
    public:
        CappedCollection(const StringData &ns, const BSONObj &options,
                         const bool mayIndexId = true) :
            NaturalOrderCollection(ns, options),
            _maxSize(options["size"].numberLong()),
            _maxObjects(options["max"].numberLong()),
            _currentObjects(0),
            _currentSize(0),
            _mutex("cappedMutex"),
            _deleteMutex("cappedDeleteMutex") {

            // Create an _id index if "autoIndexId" is missing or it exists as true.
            if (mayIndexId) {
                const BSONElement e = options["autoIndexId"];
                if (!e.ok() || e.trueValue()) {
                    BSONObj info = indexInfo(fromjson("{\"_id\":1}"), true, false);
                    createIndex(info);
                }
            }
        }
        CappedCollection(const BSONObj &serialized) :
            NaturalOrderCollection(serialized),
            _maxSize(serialized["options"]["size"].numberLong()),
            _maxObjects(serialized["options"]["max"].numberLong()),
            _currentObjects(0),
            _currentSize(0),
            _mutex("cappedMutex"),
            _deleteMutex("cappedDeleteMutex") {
            
            // Determine the number of objects and the total size.
            // We'll have to look at the data, but this might not be so bad because:
            // - you pay for it once, on initialization.
            // - capped collections are meant to be "small" (fit in memory)
            // - capped collectiosn are meant to be "read heavy",
            //   so bringing it all into memory now helps warmup.
            long long n = 0;
            long long size = 0;
            Client::Transaction txn(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
            for (shared_ptr<Cursor> c( BasicCursor::make(this) ); c->ok(); n++, c->advance()) {
                size += c->current().objsize();
            }
            txn.commit();

            _currentObjects = AtomicWord<long long>(n);
            _currentSize = AtomicWord<long long>(size);
            verify((_currentSize.load() > 0) == (_currentObjects.load() > 0));
        }

        void fillSpecificStats(BSONObjBuilder *result, int scale) const {
            result->appendBool("capped", true);
            if (_maxObjects) {
                result->appendNumber("max", _maxObjects);
            }
            result->appendNumber("cappedCount", _currentObjects.load());
            result->appendNumber("cappedSizeMax", _maxSize);
            result->appendNumber("cappedSizeCurrent", _currentSize.load());
        }

        bool isCapped() const {
            dassert(_options["capped"].trueValue());
            return true;
        }

        // @return the maximum safe key to read for a tailable cursor.
        BSONObj minUnsafeKey() {
            SimpleMutex::scoped_lock lk(_mutex);

            const long long minUncommitted = _uncommittedMinPKs.size() > 0 ?
                                             _uncommittedMinPKs.begin()->firstElement().Long() :
                                             _nextPK.load();
            TOKULOG(2) << "minUnsafeKey: minUncommitted " << minUncommitted << endl;
            BSONObjBuilder b;
            b.append("", minUncommitted);
            return b.obj();
        }

        // run an insertion where the PK is specified
        // Can come from the applier thread on a slave or a cloner 
        void insertObjectIntoCappedWithPK(BSONObj& pk, BSONObj& obj, uint64_t flags) {
            SimpleMutex::scoped_lock lk(_mutex);
            long long pkVal = pk[""].Long();
            if (pkVal >= _nextPK.load()) {
                _nextPK = AtomicWord<long long>(pkVal + 1);
            }

            // Must note the uncommitted PK before we do the actual insert,
            // since we check the capped rollback data structure to see if
            // any inserts have happened yet for this transaction (and that
            // would erroneously be true if we did the insert here first).
            noteUncommittedPK(pk);
            checkUniqueAndInsert(pk, obj, flags, true);

        }

        void insertObjectIntoCappedAndLogOps(BSONObj &obj, uint64_t flags) {
            obj = addIdField(obj);
            uassert( 16774 , str::stream() << "document is larger than capped size "
                     << obj.objsize() << " > " << _maxSize, obj.objsize() <= _maxSize );

            const BSONObj pk = getNextPK();
            checkUniqueAndInsert(pk, obj, flags | NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE, false);
            OpLogHelpers::logInsertForCapped(_ns.c_str(), pk, obj, &cc().txn());
            checkGorged(obj, true);
        }

        void insertObject(BSONObj &obj, uint64_t flags) {
            obj = addIdField(obj);
            _insertObject(obj, flags);
        }

        void deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
            msgasserted(16460, "bug: cannot remove from a capped collection, "
                               " should have been enforced higher in the stack" );
        }

        // run a deletion where the PK is specified
        // Can come from the applier thread on a slave
        void deleteObjectFromCappedWithPK(BSONObj& pk, BSONObj& obj, uint64_t flags) {
            _deleteObject(pk, obj, flags);
            // just make it easy and invalidate this
            _lastDeletedPK = BSONObj();
        }

        void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj, uint64_t flags) {
            newObj = inheritIdField(oldObj, newObj);
            long long diff = newObj.objsize() - oldObj.objsize();
            uassert( 10003 , "failing update: objects in a capped ns cannot grow", diff <= 0 );

            NamespaceDetails::updateObject(pk, oldObj, newObj, flags);
            if (diff < 0) {
                _currentSize.addAndFetch(diff);
            }
        }

    protected:
        void _insertObject(const BSONObj &obj, uint64_t flags) {
            uassert( 16328 , str::stream() << "document is larger than capped size "
                     << obj.objsize() << " > " << _maxSize, obj.objsize() <= _maxSize );

            const BSONObj pk = getNextPK();
            checkUniqueAndInsert(pk, obj, flags | NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE, false);
            checkGorged(obj, false);
        }

        // Note the commit of a transaction, which simple notes completion under the lock.
        // We don't need to do anything with nDelta and sizeDelta because those changes
        // are already applied to in-memory stats, and this transaction has committed.
        void noteCommit(const BSONObj &minPK, long long nDelta, long long sizeDelta) {
            noteComplete(minPK);
        }

        // Note the abort of a transaction, noting completion and updating in-memory stats.
        //
        // The given deltas are signed values that represent changes to the collection.
        // We need to roll back those changes. Therefore, we subtract from the current value.
        void noteAbort(const BSONObj &minPK, long long nDelta, long long sizeDelta) {
            noteComplete(minPK);
            _currentObjects.fetchAndSubtract(nDelta);
            _currentSize.fetchAndSubtract(sizeDelta);

            // If this transaction did inserts, it probably did deletes to make room
            // for the new objects. Invalidate the last key deleted so that new
            // trimming work properly recognizes that our deletes have been aborted.
            SimpleMutex::scoped_lock lk(_deleteMutex);
            _lastDeletedPK = BSONObj();
        }

    private:
        // requires: _mutex is held
        void noteUncommittedPK(const BSONObj &pk) {
            CappedCollectionRollback &rollback = cc().txn().cappedRollback();
            if (!rollback.hasNotedInsert(_ns)) {
                // This transaction has not noted an insert yet, so we save this
                // as a minimum uncommitted PK. The next insert by this txn won't be
                // the minimum, and rollback.hasNotedInsert() will be true, so
                // we won't save it.
                _uncommittedMinPKs.insert(pk.getOwned());
            }
        }

        BSONObj getNextPK() {
            SimpleMutex::scoped_lock lk(_mutex);
            BSONObjBuilder b(32);
            b.append("", _nextPK.fetchAndAdd(1));
            BSONObj pk = b.obj();
            noteUncommittedPK(pk);
            return pk;
        }

        // Note the completion of a transaction by removing its
        // minimum-PK-inserted (if there is one) from the set.
        void noteComplete(const BSONObj &minPK) {
            if (!minPK.isEmpty()) {
                SimpleMutex::scoped_lock lk(_mutex);
                const int n = _uncommittedMinPKs.erase(minPK);
                verify(n == 1);
            }
        }

        void checkGorged(const BSONObj &obj, bool logop) {
            // If the collection is gorged, we need to do some trimming work.
            long long n = _currentObjects.load();
            long long size = _currentSize.load();
            if (isGorged(n, size)) {
                trim(obj.objsize(), logop);
            }
        }

        void checkUniqueIndexes(const BSONObj &pk, const BSONObj &obj, bool checkPk) {
            dassert(!pk.isEmpty());
            dassert(!obj.isEmpty());

            // Start at 1 to skip the primary key index. We don't need to perform
            // a unique check because we always generate a unique auto-increment pk.
            int start = checkPk ? 0 : 1;
            for (int i = start; i < nIndexes(); i++) {
                IndexDetails &idx = *_indexes[i];
                if (idx.unique()) {
                    BSONObjSet keys;
                    idx.getKeysFromObject(obj, keys);
                    for (BSONObjSet::const_iterator ki = keys.begin(); ki != keys.end(); ++ki) {
                        idx.uniqueCheck(*ki, &pk);
                    }
                }
            }
        }

        // Checks unique indexes and does the actual inserts.
        // Does not check if the collection became gorged.
        void checkUniqueAndInsert(const BSONObj &pk, const BSONObj &obj, uint64_t flags, bool checkPk) {
            // Note the insert we're about to do.
            CappedCollectionRollback &rollback = cc().txn().cappedRollback();
            rollback.noteInsert(_ns, pk, obj.objsize());
            _currentObjects.addAndFetch(1);
            _currentSize.addAndFetch(obj.objsize());

            checkUniqueIndexes(pk, obj, checkPk);

            // The actual insert should not hold take any locks and does
            // not need unique checks, since we generated a unique primary
            // key and checked for uniquness constraints on secondaries above.
            insertIntoIndexes(pk, obj, flags);
        }

        bool isGorged(long long n, long long size) const {
            return (_maxObjects > 0 && n > _maxObjects) || (_maxSize > 0 && size > _maxSize);
        }

        void _deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
            // Note the delete we're about to do.
            size_t size = obj.objsize();
            CappedCollectionRollback &rollback = cc().txn().cappedRollback();
            rollback.noteDelete(_ns, pk, size);
            _currentObjects.subtractAndFetch(1);
            _currentSize.subtractAndFetch(size);

            NaturalOrderCollection::deleteObject(pk, obj, flags);
        }

        void trim(int objsize, bool logop) {
            SimpleMutex::scoped_lock lk(_deleteMutex);
            long long n = _currentObjects.load();
            long long size = _currentSize.load();
            if (isGorged(n, size)) {
                // Delete older objects until we've made enough room for the new one.
                // If other threads are trying to insert concurrently, we will do some
                // work on their behalf (until !isGorged). But we stop if we've deleted
                // K objects and done enough to satisfy our own intent, to limit latency.
                const int K = 8;
                int trimmedBytes = 0;
                int trimmedObjects = 0;
                const long long startKey = !_lastDeletedPK.isEmpty() ?
                                           _lastDeletedPK.firstElement().Long() : 0;
                // TODO: Disable prelocking on this cursor, or somehow prevent waiting 
                //       on row locks we can't get immediately.
                for ( shared_ptr<Cursor> c(IndexCursor::make(this, getPKIndex(),
                                           BSON("" << startKey), maxKey, true, 1) );
                      c->ok(); c->advance() ) {
                    BSONObj oldestPK = c->currPK();
                    BSONObj oldestObj = c->current();
                    trimmedBytes += oldestPK.objsize();
                    
                    if (logop) {
                        OpLogHelpers::logDeleteForCapped(_ns.c_str(), oldestPK, oldestObj, &cc().txn());
                    }
                    
                    // Delete the object, reload the current objects/size
                    _deleteObject(oldestPK, oldestObj, 0);
                    _lastDeletedPK = oldestPK.getOwned();
                    n = _currentObjects.load();
                    size = _currentSize.load();
                    trimmedObjects++;

                    if (!isGorged(n, size) || (trimmedBytes >= objsize && trimmedObjects >= K)) {
                        break;
                    }
                }
            }
        }

        // Remove everything from this capped collection
        void empty() {
            SimpleMutex::scoped_lock lk(_deleteMutex);
            shared_ptr<Cursor> c( BasicCursor::make(this) );
            for ( ; c->ok() ; c->advance() ) {
                _deleteObject(c->currPK(), c->current(), 0);
            }
            _lastDeletedPK = BSONObj();
        }

        const long long _maxSize;
        const long long _maxObjects;
        AtomicWord<long long> _currentObjects;
        AtomicWord<long long> _currentSize;
        BSONObj _lastDeletedPK;
        // The set of minimum-uncommitted-PKs for this capped collection.
        // Each transaction that has done inserts has the minimum PK it
        // inserted in this set.
        //
        // Tailable cursors must not read at or past the smallest value in this set.
        BSONObjSet _uncommittedMinPKs;
        SimpleMutex _mutex;
        SimpleMutex _deleteMutex;
    };

    // Profile collections are non-replicated capped collections that
    // cannot be updated and do not add the _id field on insert.
    class ProfileCollection : public CappedCollection {
    public:
        ProfileCollection(const StringData &ns, const BSONObj &options) :
            // Never automatically index the _id field
            CappedCollection(ns, options, false) {
        }
        ProfileCollection(const BSONObj &serialized) :
            CappedCollection(serialized) {
        }

        void insertObjectIntoCappedWithPK(BSONObj& pk, BSONObj& obj, uint64_t flags) {
            msgasserted( 16847, "bug: The profile collection should not have replicated inserts." );
        }

        void insertObjectIntoCappedAndLogOps(BSONObj &obj, uint64_t flags) {
            msgasserted( 16848, "bug: The profile collection should not not log inserts." );
        }

        void insertObject(BSONObj &obj, uint64_t flags) {
            _insertObject(obj, flags);
        }

        void deleteObjectFromCappedWithPK(BSONObj& pk, BSONObj& obj, uint64_t flags) {
            msgasserted( 16849, "bug: The profile collection should not have replicated deletes." );
        }

        void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj, uint64_t flags) {
            msgasserted( 16850, "bug: The profile collection should not be updated." );
        }

    private:
        void createIndex(const BSONObj &idx_info) {
            uassert(16851, "Cannot have an _id index on the system profile collection", !idx_info["key"]["_id"].ok());
        }
    };

    // A BulkLoadedCollection is a facade for an IndexedCollection that
    // utilizes a namespace builder for faster insertions. Other flavors
    // of writes are not allowed.
    //
    // The underlying indexes must be fully write locked, and they must be empty.
    // Other threads that read this collection will see nothing while it is
    // under-going load, due to MVCC. Due to the implementation, the _calling_
    // transaction will also see nothing if this collection is queried, because
    // the bulk insertions going through a loader are not immediately applied to
    // the underlying dictionaries (this specific behavior is implementation-specific).
    // Because of this, the result of querying an indexed collection on the same
    // transaction that initiated it is undefined (though non-crashing / corrupting).
    class BulkLoadedCollection : public IndexedCollection {
    public:
        BulkLoadedCollection(const BSONObj &serialized) :
            IndexedCollection(serialized),
            _bulkLoadConnectionId(cc().getConnectionId()) {
            // By noting this ns in the nsindex rollback, we will automatically
            // abort the load if the calling transaction aborts, because close()
            // will be called with aborting = true. See BulkLoadedCollection::close()
            NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
            rollback.noteNs(_ns);

            const int n = _nIndexes;
            _dbs.reset(new DB *[n]);
            _multiKeyTrackers.reset(new scoped_ptr<MultiKeyTracker>[n]);

            for (int i = 0; i < _nIndexes; i++) {
                IndexDetails &idx = *_indexes[i];
                _dbs[i] = idx.db();
                _multiKeyTrackers[i].reset(new MultiKeyTracker(_dbs[i]));
            }
            _loader.reset(new storage::Loader(_dbs.get(), n));
            _loader->setPollMessagePrefix(str::stream() << "Loader build progress: " << _ns);
        }

        void close(const bool abortingLoad) {
            try {
                if (!abortingLoad) {
                    const int r = _loader->close();
                    if (r != 0) {
                        storage::handle_ydb_error(r);
                    }
                    verify(!_indexBuildInProgress);
                    for (int i = 0; i < _nIndexes; i++) {
                        IndexDetails &idx = *_indexes[i];
                        // The PK's uniqueness is verified on loader close, so we should not check it again.
                        if (!isPKIndex(idx) && idx.unique()) {
                            checkIndexUniqueness(idx);
                        }
                        if (_multiKeyTrackers[i]->isMultiKey()) {
                            setIndexIsMultikey(i);
                        }
                    }
                }
            } catch (...) {
                _loader.reset();
                NamespaceDetails::close();
                throw;
            }
            _loader.reset();
            NamespaceDetails::close();
        }

        virtual void validateConnectionId(const ConnectionId &id) {
            uassert( 16878, str::stream() << "This connection cannot use ns " << _ns <<
                            ", it is currently under-going bulk load by connection id "
                            << _bulkLoadConnectionId,
                            _bulkLoadConnectionId == id );
        }

        void insertObject(BSONObj &obj, uint64_t flags = 0) {
            obj = addIdField(obj);
            BSONObj pk = obj["_id"].wrap("");

            storage::Key sPK(pk, NULL);
            DBT key = storage::make_dbt(sPK.buf(), sPK.size());
            DBT val = storage::make_dbt(obj.objdata(), obj.objsize());
            _loader->put(&key, &val);
        }

        void deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags = 0) {
            uasserted( 16865, "Cannot delete from a collection under-going bulk load." );
        }
        void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj, uint64_t flags = 0) {
            uasserted( 16866, "Cannot update a collection under-going bulk load." );
        }
        void empty() {
            uasserted( 16868, "Cannot empty a collection under-going bulk load." );
        }
        void optimize() {
            uasserted( 16895, "Cannot optimize a collection under-going bulk load." );
        }
        bool dropIndexes(const StringData& ns, const StringData& name, string &errmsg,
                         BSONObjBuilder &result, bool mayDeleteIdIndex) {
            uasserted( 16894, "Cannot perform drop/dropIndexes on of a collection under-going bulk load." );
        }

    private:
        void createIndex(const BSONObj &info) {
            uasserted( 16867, "Cannot create an index on a collection under-going bulk load." );
        }

        // The connection that started the bulk load is the only one that can
        // do anything with the namespace until the load is complete and this
        // namespace has been closed / re-opened.
        ConnectionId _bulkLoadConnectionId;
        scoped_array<DB *> _dbs;
        scoped_array< scoped_ptr<MultiKeyTracker> > _multiKeyTrackers;
        scoped_ptr<storage::Loader> _loader;
    };

    /* ------------------------------------------------------------------------- */

    BSONObj NamespaceDetails::indexInfo(const BSONObj &keyPattern, bool unique, bool clustering) const {
        // Can only create the _id and $_ indexes internally.
        dassert(keyPattern.nFields() == 1);
        dassert(keyPattern["_id"].ok() || keyPattern["$_"].ok());

        BSONObjBuilder b;
        b.append("ns", _ns);
        b.append("key", keyPattern);
        b.append("name", keyPattern["_id"].ok() ? "_id_" : "$_");
        if (unique) {
            b.appendBool("unique", true);
        }
        if (clustering) {
            b.appendBool("clustering", true);
        }

        BSONElement e;
        e = _options["readPageSize"];
        if (e.ok() && !e.isNull()) {
            b.append(e);
        }
        e = _options["pageSize"];
        if (e.ok() && !e.isNull()) {
            b.append(e);
        }
        e = _options["compression"];
        if (e.ok() && !e.isNull()) {
            b.append(e);
        }
        return b.obj();
    }

    static bool isSystemCatalog(const StringData &ns) {
        return ns.find(".system.indexes") != string::npos || ns.find(".system.namespaces") != string::npos;
    }
    static bool isProfileCollection(const StringData &ns) {
        return ns.find(".system.profile") != string::npos;
    }
    static bool isOplogCollection(const StringData &ns) {
        return ns == rsoplog;
    }

    // Construct a brand new NamespaceDetails with a certain primary key and set of options.
    NamespaceDetails::NamespaceDetails(const StringData &ns, const BSONObj &pkIndexPattern, const BSONObj &options) :
        _ns(ns.toString()),
        _options(options.copy()),
        _pk(pkIndexPattern.copy()),
        _indexBuildInProgress(false),
        _nIndexes(0),
        _multiKeyIndexBits(0),
        _qcWriteCount(0) {

        massert( 10356 ,  str::stream() << "invalid ns: " << ns , NamespaceString::validCollectionName(ns.rawData()));

        TOKULOG(1) << "Creating NamespaceDetails " << ns << endl;

        // Create the primary key index, generating the info from the pk pattern and options.
        BSONObj info = indexInfo(pkIndexPattern, true, true);
        createIndex(info);

        try {
            // If this throws, it's safe to call close() because we just created the index.
            // Therefore we have a write lock, and nobody else could have any uncommitted
            // modifications to this index, so close() should succeed, and #29 is irrelevant.
            addNewNamespaceToCatalog(ns, !options.isEmpty() ? &options : NULL);
        }
        catch (...) {
            close();
            throw;
        }
        computeIndexKeys();
    }
    shared_ptr<NamespaceDetails> NamespaceDetails::make(const StringData &ns, const BSONObj &options) {
        if (isOplogCollection(ns)) {
            return shared_ptr<NamespaceDetails>(new OplogCollection(ns, options));
        } else if (isSystemCatalog(ns)) {
            return shared_ptr<NamespaceDetails>(new SystemCatalogCollection(ns, options));
        } else if (isProfileCollection(ns)) {
            // TokuMX doesn't _necessarily_ need the profile to be capped, but vanilla does.
            // We enforce the restriction because it's easier to implement. See SERVER-6937.
            uassert( 16852, "System profile must be a capped collection.", options["capped"].trueValue() );
            return shared_ptr<NamespaceDetails>(new ProfileCollection(ns, options));
        } else if (options["capped"].trueValue()) {
            return shared_ptr<NamespaceDetails>(new CappedCollection(ns, options));
        } else if (options["natural"].trueValue()) {
            return shared_ptr<NamespaceDetails>(new NaturalOrderCollection(ns, options));
        } else {
            return shared_ptr<NamespaceDetails>(new IndexedCollection(ns, options));
        }
    }

    // Construct an existing NamespaceDetails given its serialized from (generated via serialize()).
    NamespaceDetails::NamespaceDetails(const BSONObj &serialized) :
        _ns(serialized["ns"].String()),
        _options(serialized["options"].Obj().copy()),
        _pk(serialized["pk"].Obj().copy()),
        _indexBuildInProgress(false),
        _nIndexes(serialized["indexes"].Array().size()),
        _multiKeyIndexBits(static_cast<uint64_t>(serialized["multiKeyIndexBits"].Long())),
        _qcWriteCount(0) {

        std::vector<BSONElement> index_array = serialized["indexes"].Array();
        for (std::vector<BSONElement>::iterator it = index_array.begin(); it != index_array.end(); it++) {
            shared_ptr<IndexDetails> idx(IndexDetails::make(it->Obj(), false));
            _indexes.push_back(idx);
        }
        computeIndexKeys();
    }
    shared_ptr<NamespaceDetails> NamespaceDetails::make(const BSONObj &serialized, const bool bulkLoad) {
        const StringData ns = serialized["ns"].Stringdata();
        if (isOplogCollection(ns)) {
            // We may bulk load the oplog since it's an IndexedCollection
            return bulkLoad ? shared_ptr<NamespaceDetails>(new BulkLoadedCollection(serialized)) :
                              shared_ptr<NamespaceDetails>(new OplogCollection(serialized));
        } else if (isSystemCatalog(ns)) {
            massert( 16869, "bug: Should not bulk load a system catalog collection", !bulkLoad );
            return shared_ptr<NamespaceDetails>(new SystemCatalogCollection(serialized));
        } else if (isProfileCollection(ns)) {
            massert( 16870, "bug: Should not bulk load the profile collection", !bulkLoad );
            return shared_ptr<NamespaceDetails>(new ProfileCollection(serialized));
        } else if (serialized["options"]["capped"].trueValue()) {
            massert( 16871, "bug: Should not bulk load capped collections", !bulkLoad );
            return shared_ptr<NamespaceDetails>(new CappedCollection(serialized));
        } else if (serialized["options"]["natural"].trueValue()) {
            massert( 16872, "bug: Should not bulk load natural order collections. ", !bulkLoad );
            return shared_ptr<NamespaceDetails>(new NaturalOrderCollection(serialized));
        } else {
            // We only know how to bulk load indexed collections.
            return bulkLoad ? shared_ptr<NamespaceDetails>(new BulkLoadedCollection(serialized)) :
                              shared_ptr<NamespaceDetails>(new IndexedCollection(serialized));
        }
    }

    void NamespaceDetails::close(const bool aborting) {
        if (!aborting) {
            verify(!_indexBuildInProgress);
        }
        for (int i = 0; i < nIndexesBeingBuilt(); i++) {
            IndexDetails &idx = *_indexes[i];
            idx.close();
        }
    }

    // Serialize the information necessary to re-open this NamespaceDetails later.
    BSONObj NamespaceDetails::serialize(const StringData& ns, const BSONObj &options, const BSONObj &pk,
            unsigned long long multiKeyIndexBits, const BSONArray &indexes_array) {
        return BSON("ns" << ns <<
                    "options" << options <<
                    "pk" << pk <<
                    "multiKeyIndexBits" << static_cast<long long>(multiKeyIndexBits) <<
                    "indexes" << indexes_array);
    }
    BSONObj NamespaceDetails::serialize() const {
        BSONArrayBuilder indexes_array;
        // Serialize all indexes that exist, including a hot index if it exists.
        for (int i = 0; i < nIndexesBeingBuilt(); i++) {
            IndexDetails &idx = *_indexes[i];
            indexes_array.append(idx.info());
        }
        return serialize(_ns, _options, _pk, _multiKeyIndexBits, indexes_array.arr());
    }

    void NamespaceDetails::computeIndexKeys() {
        _indexKeys.clear();
        NamespaceDetails::IndexIterator i = ii();
        while ( i.more() ) {
            i.next().keyPattern().getFieldNames(_indexKeys);
        }
    }

    void NamespaceDetails::resetTransient() {
        Lock::assertWriteLocked(_ns); 
        clearQueryCache();
        computeIndexKeys();
    }

    void NamespaceDetails::clearQueryCache() {
        QueryCacheRWLock::Exclusive lk(this);
        _qcCache.clear();
        _qcWriteCount = 0;
    }

    void NamespaceDetails::notifyOfWriteOp() {
        if ( _qcCache.empty() ) {
            return;
        }
        if ( ++_qcWriteCount >= 100 ) {
            clearQueryCache();
        }
    }

    CachedQueryPlan NamespaceDetails::cachedQueryPlanForPattern( const QueryPattern &pattern ) {
        map<QueryPattern, CachedQueryPlan>::const_iterator i = _qcCache.find(pattern);
        return i != _qcCache.end() ? i->second : CachedQueryPlan();
    }

    void NamespaceDetails::registerCachedQueryPlanForPattern( const QueryPattern &pattern,
                                            const CachedQueryPlan &cachedQueryPlan ) {
        _qcCache[ pattern ] = cachedQueryPlan;
    }

    int NamespaceDetails::findByPKCallback(const DBT *key, const DBT *value, void *extra) {
        struct findByPKCallbackExtra *info = reinterpret_cast<findByPKCallbackExtra *>(extra);
        try {
            if (key != NULL) {
                struct findByPKCallbackExtra *info = reinterpret_cast<findByPKCallbackExtra *>(extra);
                BSONObj obj(reinterpret_cast<char *>(value->data));
                info->obj = obj.getOwned();
            }
            return 0;
        } catch (std::exception &e) {
            info->ex = &e;
        }
        return -1;
    }

    bool NamespaceDetails::findOne(const BSONObj &query, BSONObj &result, const bool requireIndex) const {
        for (shared_ptr<Cursor> c( getOptimizedCursor(_ns, query, BSONObj(),
                                       requireIndex ? QueryPlanSelectionPolicy::indexOnly() :
                                                      QueryPlanSelectionPolicy::any() ) );
             c->ok(); c->advance()) {
            if ( c->currentMatches() && !c->getsetdup( c->currPK() ) ) {
                result = c->current().copy();
                return true;
            }
        }
        return false;
    }

    bool NamespaceDetails::findByPK(const BSONObj &key, BSONObj &result) const {

        // get a cursor over the primary key index
        IndexDetails &pkIdx = getPKIndex();
        IndexDetails::Cursor c(pkIdx);
        DBC *cursor = c.dbc();

        // create an index key
        storage::Key sKey(key, NULL);
        DBT key_dbt = sKey.dbt();

        TOKULOG(3) << "NamespaceDetails::findByPK looking for " << key << endl;

        // TODO: Use db->getf_set which does less malloc and free.
        // Try to find it.
        BSONObj obj = BSONObj();
        struct findByPKCallbackExtra extra(obj);
        const int r = cursor->c_getf_set(cursor, 0, &key_dbt, findByPKCallback, &extra);
        if (extra.ex != NULL) {
            throw *extra.ex;
        }
        if (r != 0 && r != DB_NOTFOUND) {
            storage::handle_ydb_error(r);
        }

        if (!obj.isEmpty()) {
            result = obj;
            return true;
        } else {
            return false;
        }
    }

    // TODO: Cache two of these in the client object so they're not created/destroyed
    //       every time a connection/transaction does a single write. (multi inserts,
    //       updates, deletes should come to mind)
    class DBTArraysHolder : boost::noncopyable {
    public:
        DBTArraysHolder(DBT_ARRAY *dbt_arrays, const size_t n) :
            _dbt_arrays(dbt_arrays),
            _n(n) {
            memset(dbt_arrays, 0, n * sizeof(DBT_ARRAY));
        }
        ~DBTArraysHolder() {
            for (size_t i = 0; i < _n; i++) {
                DBT_ARRAY *dbt_array = &_dbt_arrays[i];
                for (size_t j = 0; j < dbt_array->capacity; j++) {
                    DBT *dbt = &dbt_array->dbts[j];
                    if (dbt->data != NULL && dbt->flags == DB_DBT_REALLOC) {
                        free(dbt->data);
                        dbt->data = NULL;
                    }
                }
                if (dbt_array->dbts != NULL) {
                    free(dbt_array->dbts);
                    dbt_array->dbts = NULL;
                }
            }
        }
    private:
        DBT_ARRAY *const _dbt_arrays;
        const size_t _n;
    };

    void NamespaceDetails::insertIntoIndexes(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        dassert(!pk.isEmpty());
        dassert(!obj.isEmpty());

        const int n = nIndexesBeingBuilt();
        DB *dbs[n];
        DBT_ARRAY key_dbt_arrays[n];
        DBT_ARRAY val_dbt_arrays[n];
        DBTArraysHolder keyArraysHolder(key_dbt_arrays, n);
        DBTArraysHolder valArraysHolder(val_dbt_arrays, n);
        uint32_t put_flags[n];

        storage::Key sPK(pk, NULL);
        DBT src_key = storage::make_dbt(sPK.buf(), sPK.size());
        DBT src_val = storage::make_dbt(obj.objdata(), obj.objsize());

        for (int i = 0; i < n; i++) {
            const bool isPK = i == 0;
            const bool prelocked = flags & NamespaceDetails::NO_LOCKTREE;
            const bool noUniqueChecks = flags & NamespaceDetails::NO_UNIQUE_CHECKS;
            IndexDetails &idx = *_indexes[i];
            dbs[i] = idx.db();
            put_flags[i] = (isPK && !noUniqueChecks ? DB_NOOVERWRITE : 0) |
                           (prelocked ? DB_PRELOCKED_WRITE : 0);

            // TODO: We need to generate keys here to check uniqueness and possible
            //       set the multikey bit for an index. Find a way to store the keys
            //       generated in the above DBT_ARRAYs in such a way that the we can
            //       detect in the generate_row_for_put callback that keys do not
            //       need to be generated.
            //
            //       This should be as easy as setting the DBTs here and checking for
            //       a non-empty DBT_ARRAY in the generate callback. Only question:
            //       does the ydb ever give the generate callback a non empty DBT_ARRAY?
            //       Maybe during recovery? It shouldn't.

            BSONObjSet idxKeys;
            if (!isPK) {
                idx.getKeysFromObject(obj, idxKeys);
                if (idx.unique() && !noUniqueChecks) {
                    for (BSONObjSet::const_iterator o = idxKeys.begin(); o != idxKeys.end(); ++o) {
                        idx.uniqueCheck(*o, &pk);
                    }
                }
                if (idxKeys.size() > 1) {
                    setIndexIsMultikey(i);
                }
            }
        }

        DB_ENV *env = storage::env;
        const int r = env->put_multiple(env, dbs[0], cc().txn().db_txn(),
                                        &src_key, &src_val,
                                        n, dbs, key_dbt_arrays, val_dbt_arrays, put_flags);
        if (r == EINVAL) {
            uasserted( 16900, str::stream() << "Indexed insertion failed." <<
                              " This may be due to keys > 32kb. Check the error log." );
        } else if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    void NamespaceDetails::deleteFromIndexes(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        dassert(!pk.isEmpty());
        dassert(!obj.isEmpty());

        const int n = nIndexesBeingBuilt();
        DB *dbs[n];
        DBT_ARRAY key_dbt_arrays[n];
        DBT_ARRAY val_dbt_arrays[n];
        DBTArraysHolder keyArraysHolder(key_dbt_arrays, n);
        DBTArraysHolder valArraysHolder(val_dbt_arrays, n);
        uint32_t del_flags[n];

        storage::Key sPK(pk, NULL);
        DBT src_key = storage::make_dbt(sPK.buf(), sPK.size());
        DBT src_val = storage::make_dbt(obj.objdata(), obj.objsize());

        for (int i = 0; i < n; i++) {
            const bool prelocked = flags & NamespaceDetails::NO_LOCKTREE;
            IndexDetails &idx = *_indexes[i];
            dbs[i] = idx.db();
            del_flags[i] = DB_DELETE_ANY | (prelocked ? DB_PRELOCKED_WRITE : 0);
        }

        DB_ENV *env = storage::env;
        const int r = env->del_multiple(env, dbs[0], cc().txn().db_txn(),
                                        &src_key, &src_val,
                                        n, dbs, key_dbt_arrays, del_flags);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    // uasserts on duplicate key
    static bool orderedSetContains(const BSONObjSet &set, const BSONObj &obj) {
        bool contains = false;
        for (BSONObjSet::iterator i = set.begin(); i != set.end(); i++) {
            const int c = i->woCompare(obj);
            if (c >= 0) {
                contains = c == 0;
                break;
            }
        }
        return contains;
    }

    // deletes an object from this namespace, taking care of secondary indexes if they exist
    void NamespaceDetails::deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        deleteFromIndexes(pk, obj, flags);
    }

    void NamespaceDetails::updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj, uint64_t flags) {
        TOKULOG(4) << "NamespaceDetails::updateObject pk "
            << pk << ", old " << oldObj << ", new " << newObj << endl;

        dassert(!pk.isEmpty());
        dassert(!oldObj.isEmpty());
        dassert(!newObj.isEmpty());

        const int n = nIndexesBeingBuilt();
        DB *dbs[n];
        DBT_ARRAY key_dbt_arrays[n * 2];
        DBT_ARRAY val_dbt_arrays[n];
        DBTArraysHolder keysArraysHolder(key_dbt_arrays, n * 2);
        DBTArraysHolder valsArraysHolder(val_dbt_arrays, n);
        uint32_t update_flags[n];

        storage::Key sPK(pk, NULL);
        DBT src_key = storage::make_dbt(sPK.buf(), sPK.size());
        DBT new_src_val = storage::make_dbt(newObj.objdata(), newObj.objsize());
        DBT old_src_val = storage::make_dbt(oldObj.objdata(), oldObj.objsize());

        // Generate keys for each index, prepare data structures for del multiple.
        // We will end up abandoning del multiple if there are any multikey indexes.
        for (int i = 0; i < n; i++) {
            const bool isPK = i == 0;
            const bool prelocked = flags & NamespaceDetails::NO_LOCKTREE;
            const bool noUniqueChecks = flags & NamespaceDetails::NO_UNIQUE_CHECKS;
            IndexDetails &idx = *_indexes[i];
            dbs[i] = idx.db();
            update_flags[i] = prelocked ? DB_PRELOCKED_WRITE : 0;

            if (!isPK) {
                BSONObjSet oldIdxKeys;
                BSONObjSet newIdxKeys;
                idx.getKeysFromObject(oldObj, oldIdxKeys);
                idx.getKeysFromObject(newObj, newIdxKeys);
                if (idx.unique() && !noUniqueChecks) {
                    // Only perform the unique check if the key actually changed.
                    for (BSONObjSet::iterator o = newIdxKeys.begin(); o != newIdxKeys.end(); ++o) {
                        const BSONObj &k = *o;
                        if (!orderedSetContains(oldIdxKeys, k)) {
                            idx.uniqueCheck(k, &pk);
                        }
                    }
                }
                if (newIdxKeys.size() > 1) {
                    setIndexIsMultikey(i);
                }
            }
        }

        // The pk doesn't change, so old_src_key == new_src_key.
        DB_ENV *env = storage::env;
        const int r = env->update_multiple(env, dbs[0], cc().txn().db_txn(),
                                           &src_key, &old_src_val,
                                           &src_key, &new_src_val,
                                           n, dbs, update_flags,
                                           n * 2, key_dbt_arrays, n, val_dbt_arrays);
        if (r == EINVAL) {
            uasserted( 16908, str::stream() << "Indexed insertion (on update) failed." <<
                              " This may be due to keys > 32kb. Check the error log." );
        } else if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    void NamespaceDetails::setIndexIsMultikey(const int idxNum) {
        dassert(idxNum < NIndexesMax);
        const unsigned long long x = ((unsigned long long) 1) << idxNum;
        if (_multiKeyIndexBits & x) {
            return;
        }
        if (!Lock::isWriteLocked(_ns)) {
            throw RetryWithWriteLock();
        }

        _multiKeyIndexBits |= x;
        nsindex(_ns)->update_ns(_ns, serialize(), true);
        resetTransient();
    }

    void NamespaceDetails::checkIndexUniqueness(const IndexDetails &idx) {
        IndexScanCursor c(this, idx, 1);
        BSONObj prevKey = c.currKey().getOwned();
        c.advance();
        for ( ; c.ok(); c.advance() ) {
            BSONObj currKey = c.currKey(); 
            if (currKey == prevKey) {
                idx.uassertedDupKey(currKey);
            }
            prevKey = currKey.getOwned();
        }
    }

    void NamespaceDetails::empty() {
        for ( shared_ptr<Cursor> c( BasicCursor::make(this) ); c->ok() ; c->advance() ) {
            deleteObject(c->currPK(), c->current(), 0);
        }
        resetTransient();
    }

    // Wrapper for offline (write locked) indexing.
    void NamespaceDetails::createIndex(const BSONObj &info) {
        if (!Lock::isWriteLocked(_ns)) {
            throw RetryWithWriteLock();
        }

        ColdIndexer indexer(this, info);
        indexer.prepare();
        indexer.build();
        indexer.commit();
    }

    void NamespaceDetails::dropIndex(const int idxNum) {
        verify(idxNum < (int) _indexes.size());
        IndexDetails &idx = *_indexes[idxNum];
        idx.kill_idx();
        _indexes.erase(_indexes.begin() + idxNum);
        _nIndexes--;
        resetTransient();
    }

    // Normally, we cannot drop the _id_ index.
    // The parameters mayDeleteIdIndex is here for the case where we call dropIndexes
    // through dropCollection, in which case we are dropping an entire collection,
    // hence the _id_ index will have to go.
    bool NamespaceDetails::dropIndexes(const StringData& ns, const StringData& name, string &errmsg, BSONObjBuilder &result, bool mayDeleteIdIndex) {
        Lock::assertWriteLocked(ns);
        TOKULOG(1) << "dropIndexes " << name << endl;

        // Note this ns in the rollback so if this transaction aborts, we'll
        // close this ns, forcing the next user to reload in-memory metadata.
        NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
        rollback.noteNs(_ns);

        NamespaceDetails *d = nsdetails(ns);
        ClientCursor::invalidate(ns);

        const int idxNum = findIndexByName(name);
        if (_indexBuildInProgress &&
            (name == "*" || idxNum == (int) _indexes.size() - 1)) {
            uasserted( 16904, "Cannot drop index: build in progress." );
        }

        if (name == "*") {
            result.append("nIndexesWas", (double) _nIndexes);
            // This is O(n^2), not great, but you can have at most 64 indexes anyway.
            for (int i = 0; i < _nIndexes; ) {
                IndexDetails &idx = *_indexes[i];
                if (mayDeleteIdIndex || (!idx.isIdIndex() && !d->isPKIndex(idx))) {
                    dropIndex(i);
                } else {
                    i++;
                }
            }
            // Assuming id index isn't multikey
            _multiKeyIndexBits = 0;
            result.append("msg", (mayDeleteIdIndex
                                  ? "indexes dropped for collection"
                                  : "non-_id indexes dropped for collection"));
        } else {
            if (idxNum >= 0) {
                result.append("nIndexesWas", (double) _nIndexes);
                IndexVector::iterator it = _indexes.begin() + idxNum;
                IndexDetails *idx = it->get();
                if ( !mayDeleteIdIndex && (idx->isIdIndex() || d->isPKIndex(*idx)) ) {
                    errmsg = "may not delete _id or $_ index";
                    return false;
                }
                dropIndex(idxNum);
                // Removes the nth bit, and shifts any bits higher than it down a slot.
                _multiKeyIndexBits = ((_multiKeyIndexBits & ((1ULL << idxNum) - 1)) |
                                     ((_multiKeyIndexBits >> (idxNum + 1)) << idxNum));
            } else {
                log() << "dropIndexes: " << name << " not found" << endl;
                errmsg = "index not found";
                return false;
            }
        }
        // Updated whatever in memory structures are necessary, now update the nsindex.
        nsindex(ns)->update_ns(ns, serialize(), true);
        return true;
    }

    void NamespaceDetails::fillIndexStats(std::vector<IndexStats> &indexStats) const {
        for (int i = 0; i < _nIndexes; i++) {
            IndexDetails &idx = *_indexes[i];
            IndexStats stats(idx);
            indexStats.push_back(stats);
        }
    }

    void NamespaceDetails::optimize() {
        for (int i = 0; i < _nIndexes; i++) {
            IndexDetails &idx = *_indexes[i];
            idx.optimize();
        }
    }

    void NamespaceDetails::fillCollectionStats(
        struct NamespaceDetailsAccStats* accStats, 
        BSONObjBuilder* result, 
        int scale) const  
    {
        uint32_t numIndexes = nIndexes();
        accStats->nIndexes = numIndexes;
        std::vector<IndexStats> indexStats;
        // fill each of the indexStats with statistics
        fillIndexStats(indexStats);
        // also sum up some stats of secondary indexes,
        // calculate their total data size and storage size
        uint64_t totalIndexDataSize = 0;
        uint64_t totalIndexStorageSize = 0;
        BSONArrayBuilder index_info;
        for (std::vector<IndexStats>::const_iterator it = indexStats.begin(); it != indexStats.end(); ++it) {
            index_info.append(it->bson(scale));
            // the primary key is at indexStats[0], secondary indexes come after
            if (it - indexStats.begin() > 0) {
                totalIndexDataSize += it->getDataSize();
                totalIndexStorageSize += it->getStorageSize();
            }
        }

        accStats->count = indexStats[0].getCount();
        result->appendNumber("count", (long long) accStats->count);

        result->append("nindexes" , numIndexes );
        result->append("nindexesbeingbuilt" , nIndexesBeingBuilt() );

        accStats->size = indexStats[0].getDataSize();
        result->appendNumber("size", (long long) accStats->size/scale);

        accStats->storageSize = indexStats[0].getStorageSize();
        result->appendNumber("storageSize", (long long) accStats->storageSize/scale);

        accStats->indexSize = totalIndexDataSize;
        result->appendNumber("totalIndexSize", (long long) totalIndexDataSize/scale);

        accStats->indexStorageSize = totalIndexStorageSize;
        result->appendNumber("totalIndexStorageSize", (long long) totalIndexStorageSize/scale);

        result->append("indexDetails", index_info.arr());        

        fillSpecificStats(result, scale);
    }

    static void addIndexToCatalog(const BSONObj &info) {
        const StringData &indexns = info["ns"].Stringdata();
        if (indexns.find(".system.indexes") != string::npos) {
            // system.indexes holds all the others, so it is not explicitly listed in the catalog.
            return;
        }

        StringData database = nsToDatabaseSubstring(indexns);
        string ns = database.toString() + ".system.indexes";
        NamespaceDetails *d = nsdetails_maybe_create(ns);
        BSONObj objMod = info;
        insertOneObject(d, objMod);
    }

    void NamespaceDetails::addDefaultIndexesToCatalog() {
        // Either a single primary key or a hidden primary key + _id index.
        dassert(_nIndexes == 1 || (_nIndexes == 2 && findIdIndex() == 1));
        for (int i = 0; i < nIndexes(); i++) {
            addIndexToCatalog(_indexes[i]->info());
        }
    }

    bool NamespaceDetails::ensureIndex(const BSONObj &info) {
        const BSONObj keyPattern = info["key"].Obj();
        const int i = findIndexByKeyPattern(keyPattern);
        if (i >= 0) {
            return false;
        }
        createIndex(info);
        return true;
    }

    void NamespaceDetails::acquireTableLock() {
        verify(!_indexBuildInProgress);
        for (int i = 0; i < _nIndexes; i++) {
            IndexDetails &idx = *_indexes[i];
            idx.acquireTableLock();
        }
    }

    /* ------------------------------------------------------------------------- */

    // TODO: All global functions manipulating namespaces should be static in NamespaceDetails

    static void checkConfigNS(const StringData& ns) {
        if ( cmdLine.configsvr &&
             !( ns.startsWith( "config." ) ||
                ns.startsWith( "local." ) ||
                ns.startsWith( "admin." ) ) ) {
            uasserted(14037, "can't create user databases on a --configsvr instance");
        }
    }

    bool userCreateNS(const StringData& ns, BSONObj options, string& err, bool logForReplication) {
        StringData coll = ns.substr(ns.find('.') + 1);
        massert( 16451 ,  str::stream() << "invalid ns: " << ns , NamespaceString::validCollectionName(ns.rawData()));
        StringData cl = nsToDatabaseSubstring( ns );
        if (nsdetails(ns) != NULL) {
            // Namespace already exists
            err = "collection already exists";
            return false;
        }

        checkConfigNS(ns);

        {
            BSONElement e = options.getField("size");
            if (e.isNumber()) {
                long long size = e.numberLong();
                uassert(10083, "create collection invalid size spec", size > 0);
            }
        }

        // This creates the namespace as well as its _id index
        nsdetails_maybe_create(ns, options);
        if ( logForReplication ) {
            if ( options.getField( "create" ).eoo() ) {
                BSONObjBuilder b;
                b << "create" << coll;
                b.appendElements( options );
                options = b.obj();
            }
            string logNs = cl.toString() + ".$cmd";
            OpLogHelpers::logCommand(logNs.c_str(), options, &cc().txn());
        }
        // TODO: Identify error paths for this function
        return true;
    }

    NamespaceDetails* getAndMaybeCreateNS(const StringData& ns, bool logop) {
        NamespaceDetails* details = nsdetails(ns);
        if (details == NULL) {
            string err;
            BSONObj options;
            bool created = userCreateNS(ns, options, err, logop);
            uassert(16745, "failed to create collection", created);
            details = nsdetails(ns);
            uassert(16746, "failed to get collection after creating", details);
        }
        return details;
    }

    void dropDatabase(const StringData& name) {
        TOKULOG(1) << "dropDatabase " << name << endl;
        Lock::assertWriteLocked(name);
        Database *d = cc().database();
        verify(d != NULL);
        verify(d->name() == name);

        // Disable dropDatabase in a multi-statement transaction until
        // we have the time/patience to test/debug it.
        if (cc().txnStackSize() > 1) {
            uasserted(16777, "Cannot dropDatabase in a multi-statement transaction.");
        }

        nsindex(name)->drop();
        Database::closeDatabase(d->name().c_str(), d->path());
    }

    // TODO: Put me in NamespaceDetails
    void dropCollection(const StringData& name, string &errmsg, BSONObjBuilder &result, bool can_drop_system) {
        TOKULOG(1) << "dropCollection " << name << endl;
        NamespaceDetails *d = nsdetails(name);
        if (d == NULL) {
            return;
        }

        // Check that we are allowed to drop the namespace.
        StringData database = nsToDatabaseSubstring(name);
        verify(database == cc().database()->name());
        StringData coll = name.substr(name.find('.') + 1);
        if (coll.startsWith("system.")) {
            if (coll == "system.profile") {
                uassert(10087, "turn off profiling before dropping system.profile collection", cc().database()->profile() == 0);
            } else if (!can_drop_system) {
                uasserted(12502, "can't drop system ns");
            }
        }

        // Invalidate cursors, then drop all of the indexes.
        ClientCursor::invalidate(name.rawData());

        LOG(1) << "\t dropIndexes done" << endl;
        d->dropIndexes(name, "*", errmsg, result, true);
        verify(d->nIndexes() == 0);
        removeNamespaceFromCatalog(name);

        // If everything succeeds, kill the namespace from the nsindex.
        Top::global.collectionDropped(name);
        nsindex(name)->kill_ns(name);
        result.append("ns", name);
    }

    /* add a new namespace to the system catalog (<dbname>.system.namespaces).
       options: { capped : ..., size : ... }
    */
    void addNewNamespaceToCatalog(const StringData& ns, const BSONObj *options) {
        LOG(1) << "New namespace: " << ns << endl;
        if (ns.find(".system.namespaces") != string::npos) {
            // system.namespaces holds all the others, so it is not explicitly listed in the catalog.
            return;
        }

        BSONObjBuilder b;
        b.append("name", ns);
        if ( options ) {
            b.append("options", *options);
        }
        BSONObj info = b.done();

        StringData database = nsToDatabaseSubstring(ns);
        string system_ns = database.toString() + ".system.namespaces";
        NamespaceDetails *d = nsdetails_maybe_create(system_ns);
        insertOneObject(d, info);
    }

    void removeNamespaceFromCatalog(const StringData& ns) {
        if (ns.find(".system.namespaces") == string::npos) {
            string system_namespaces = cc().database()->name() + ".system.namespaces";
            _deleteObjects(system_namespaces.c_str(),
                           BSON("name" << ns), false, false);
        }
    }

    void removeFromSysIndexes(const StringData& ns, const StringData& name) {
        string system_indexes = cc().database()->name() + ".system.indexes";
        BSONObj obj = BSON("ns" << ns << "name" << name);
        TOKULOG(2) << "removeFromSysIndexes removing " << obj << endl;
        const int n = _deleteObjects(system_indexes.c_str(), obj, false, false);
        verify(n == 1);
    }

    static BSONObj replaceNSField(const BSONObj &obj, const StringData &to) {
        BSONObjBuilder b;
        BSONObjIterator i( obj );
        while ( i.more() ) {
            BSONElement e = i.next();
            if ( strcmp( e.fieldName(), "ns" ) != 0 ) {
                b.append( e );
            } else {
                b << "ns" << to;
            }
        }
        return b.obj();
    }

    void renameNamespace(const StringData& from, const StringData& to, bool stayTemp) {
        Lock::assertWriteLocked(from);
        verify( nsdetails(from) != NULL );
        verify( nsdetails(to) == NULL );

        uassert( 16896, "Cannot rename a collection under-going bulk load.", from != cc().bulkLoadNS() );

        // Invalidate any existing cursors on the old namespace details,
        // and reset the query cache.
        ClientCursor::invalidate( from );

        StringData database = nsToDatabaseSubstring(from);
        string sysIndexes = database.toString() + ".system.indexes";
        string sysNamespaces = database.toString() + ".system.namespaces";

        // Generate the serialized form of the namespace, and then close it.
        // This will close the underlying dictionaries and allow us to
        // rename them in the environment.
        BSONObj serialized = nsdetails(from)->serialize();
        bool closed = nsindex(from)->close_ns(from);
        verify(closed);

        // Rename each index in system.indexes and system.namespaces
        {
            BSONObj nsQuery = BSON( "ns" << from );
            vector<BSONObj> indexSpecs;
            {
                // Find all entries in sysIndexes for the from ns
                Client::Context ctx( sysIndexes );
                for ( shared_ptr<Cursor> c( getOptimizedCursor( sysIndexes, nsQuery ) );
                      c->ok(); c->advance() ) {
                    if (c->currentMatches()) {
                        indexSpecs.push_back( c->current().copy() );
                    }
                }
            }
            for ( vector<BSONObj>::const_iterator it = indexSpecs.begin() ; it != indexSpecs.end(); it++) {
                BSONObj oldIndexSpec = *it;
                string idxName = oldIndexSpec["name"].String();
                string oldIdxNS = IndexDetails::indexNamespace(from, idxName);
                string newIdxNS = IndexDetails::indexNamespace(to, idxName);

                TOKULOG(1) << "renaming " << oldIdxNS << " to " << newIdxNS << endl;
                storage::db_rename(oldIdxNS, newIdxNS);

                BSONObj newIndexSpec = replaceNSField( oldIndexSpec, to );
                addIndexToCatalog( newIndexSpec );
                addNewNamespaceToCatalog( newIdxNS, newIndexSpec.isEmpty() ? 0 : &newIndexSpec );
                _deleteObjects( sysNamespaces.c_str(), BSON( "name" << oldIdxNS ), false, false );
            }
            // Clean out the old entries from system.indexes. We already removed them
            // from system.namespaces in the loop above.
            _deleteObjects( sysIndexes.c_str(), nsQuery, false, false);
        }

        // Rename the namespace in system.namespaces
        BSONObj newSpec;
        {
            BSONObj oldSpec;
            NamespaceDetails *d = nsdetails( sysNamespaces.c_str() );
            verify( d != NULL && d->findOne( BSON( "name" << from ), oldSpec ) );
            BSONObjBuilder b;
            BSONObjIterator i( oldSpec.getObjectField( "options" ) );
            while ( i.more() ) {
                BSONElement e = i.next();
                if ( strcmp( e.fieldName(), "create" ) != 0 ) {
                    if (stayTemp || (strcmp(e.fieldName(), "temp") != 0)) {
                        b.append( e );
                    }
                }
                else {
                    b << "create" << to;
                }
            }
            newSpec = b.obj();
            addNewNamespaceToCatalog( to, newSpec.isEmpty() ? 0 : &newSpec );
            _deleteObjects( sysNamespaces.c_str(), BSON( "name" << from ), false, false );
        }

        // Update the namespace index
        {
            BSONArrayBuilder newIndexesArray;
            vector<BSONElement> indexes = serialized["indexes"].Array();
            for (vector<BSONElement>::const_iterator it = indexes.begin(); it != indexes.end(); it++) {
                newIndexesArray.append( replaceNSField( it->Obj(), to ) );
            }
            BSONObj newSerialized = NamespaceDetails::serialize( to, newSpec, serialized["pk"].Obj(),
                                                                 serialized["multiKeyIndexBits"].Long(),
                                                                 newIndexesArray.arr());
            // Kill the old entry and replace it with the new name and modified spec.
            // The next user of the newly-named namespace will need to open it.
            NamespaceIndex *ni = nsindex( from );
            ni->kill_ns( from );
            ni->update_ns( to, newSerialized, false );
            verify( nsdetails(to) != NULL );
            verify( nsdetails(from) == NULL );
        }
    }

    void beginBulkLoad(const StringData &ns, const vector<BSONObj> &indexes,
                       const BSONObj &options) {
        uassert( 16873, "Cannot bulk load a collection that already exists.",
                        nsdetails(ns) == NULL );
        uassert( 16998, "Cannot bulk load a system collection",
                        ns.find(".system.") == string::npos );
        uassert( 16999, "Cannot bulk load a capped collection",
                        !options["capped"].trueValue() );
        uassert( 17000, "Cannot bulk load a natural order collection",
                        !options["natural"].trueValue() );

        // Don't log the create. The begin/commit/abort load commands are already logged.
        string errmsg;
        const bool created = userCreateNS(ns, options, errmsg, false);
        verify(created);

        NamespaceIndex *ni = nsindex(ns);
        NamespaceDetails *d = ni->details(ns);
        d->acquireTableLock();
        for (vector<BSONObj>::const_iterator i = indexes.begin(); i != indexes.end(); i++) {
            BSONObj info = *i;
            const BSONElement &e = info["ns"];
            if (e.ok()) {
                uassert( 16886, "Each index spec's ns field, if provided, must match the loaded ns.",
                                e.type() == mongo::String && e.Stringdata() == ns );
            } else {
                // Add the ns field if it wasn't provided.
                BSONObjBuilder b;
                b.append("ns", ns);
                b.appendElements(info);
                info = b.obj();
            }
            uassert( 16887, "Each index spec must have a string name field.",
                            info["name"].ok() && info["name"].type() == mongo::String );
            if (d->ensureIndex(info)) {
                addIndexToCatalog(info);
            }
        }

        // Now the ns exists. Close it and re-open it in "bulk load" mode.
        const bool closed = ni->close_ns(ns);
        verify(closed);
        const bool opened = ni->open_ns(ns, true);
        verify(opened);
    }

    void commitBulkLoad(const StringData &ns) {
        NamespaceIndex *ni = nsindex(ns);
        const bool closed = ni->close_ns(ns);
        verify(closed);
    }

    void abortBulkLoad(const StringData &ns) {
        NamespaceIndex *ni = nsindex(ns);
        // Close the ns with aborting = true, which will hint to the
        // BulkLoadedCollection that it should abort the load.
        const bool closed = ni->close_ns(ns, true);
        verify(closed);
    }

    bool legalClientSystemNS( const StringData& ns , bool write ) {
        if( ns == "local.system.replset" ) return true;

        if ( ns.find( ".system.users" ) != string::npos )
            return true;

        if ( ns.find( ".system.js" ) != string::npos ) {
            if ( write )
                Scope::storedFuncMod();
            return true;
        }

        return false;
    }

} // namespace mongo
