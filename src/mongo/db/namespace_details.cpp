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
#include "mongo/db/dbhelpers.h"
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

    NamespaceIndex *nsindex(const char *ns) {
        Database *database = cc().database();
        verify( database );
        DEV {
            char buf[256];
            nsToDatabase(ns, buf);
            if ( database->name() != buf ) {
                out() << "ERROR: attempt to write to wrong database\n";
                out() << " ns:" << ns << '\n';
                out() << " database->name:" << database->name() << endl;
                verify( database->name() == buf );
            }
        }
        return &database->_nsIndex;
    }

    NamespaceDetails *nsdetails(const char *ns) {
        return nsindex(ns)->details(ns);
    }

    NamespaceDetails *nsdetails_maybe_create(const char *ns, BSONObj options) {
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

            Namespace ns_s(ns);
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
        IndexedCollection(const string &ns, const BSONObj &options) :
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
        OplogCollection(const string &ns, const BSONObj &options) :
            IndexedCollection(ns, options) {
        } 
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
    };

    struct getfExtra {
        getfExtra(BSONObj &k) : key(k) {
        }
        BSONObj &key;
    };

    static int getfCallback(const DBT *key, const DBT *value, void *extra) {
        struct getfExtra *info = reinterpret_cast<struct getfExtra *>(extra);
        if (key) {
            const storage::Key sKey(key);
            info->key = sKey.key().getOwned();
        }
        return 0;
    }

    class NaturalOrderCollection : public NamespaceDetails {
    public:
        NaturalOrderCollection(const string &ns, const BSONObj &options) :
            NamespaceDetails(ns, fromjson("{\"$_\":1}"), options),
            _nextPK(0) {
        }
        NaturalOrderCollection(const BSONObj &serialized) :
            NamespaceDetails(serialized),
            _nextPK(0) {

            // the next PK, if it exists, is the last key + 1
            int r;
            IndexDetails &pkIdx = getPKIndex();
            Client::Transaction txn(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
            {
                IndexDetails::Cursor c(pkIdx);
                DBC *cursor = c.dbc();

                BSONObj key = BSONObj();
                struct getfExtra extra(key);
                r = cursor->c_getf_last(cursor, 0, getfCallback, &extra);
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
    };

    class SystemCatalogCollection : public NaturalOrderCollection {
    public:
        SystemCatalogCollection(const string &ns, const BSONObj &options) :
            NaturalOrderCollection(ns, options) {
        }
        SystemCatalogCollection(const BSONObj &serialized) :
            NaturalOrderCollection(serialized) {
        }

        // strip out the _id field before inserting into a system collection
        void insertObject(BSONObj &obj, uint64_t flags) {
            obj = beautify(obj);
            NaturalOrderCollection::insertObject(obj, flags);
        }

        void createIndex(const BSONObj &info) {
            massert(16464, "bug: system collections should not be indexed.", false);
        }

    private:
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
        CappedCollection(const string &ns, const BSONObj &options) :
            NaturalOrderCollection(ns, options),
            _maxSize(options["size"].numberLong()),
            _maxObjects(options["max"].numberLong()),
            _currentObjects(0),
            _currentSize(0),
            _mutex("cappedMutex"),
            _deleteMutex("cappedDeleteMutex") {

            // Create an _id index if "autoIndexId" is missing or it exists as true.
            const BSONElement e = options["autoIndexId"];
            if (!e.ok() || e.trueValue()) {
                BSONObj info = indexInfo(fromjson("{\"_id\":1}"), true, false);
                createIndex(info);
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
            for (scoped_ptr<Cursor> c( BasicCursor::make(this) ); c->ok(); n++, c->advance()) {
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
        virtual void insertObjectIntoCappedWithPK(BSONObj& pk, BSONObj& obj, uint64_t flags) {
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
            _insertObject(pk, obj, flags, true);

        }

        virtual void insertObjectIntoCappedAndLogOps(BSONObj &obj, uint64_t flags) {
            obj = addIdField(obj);
            uassert( 16774 , str::stream() << "document is larger than capped size "
                     << obj.objsize() << " > " << _maxSize, obj.objsize() <= _maxSize );

            BSONObj pk = getNextPK();
            _insertObject(pk, obj, flags | NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE, false);
            OpLogHelpers::logInsertForCapped(_ns.c_str(), pk, obj, &cc().txn());

            // If the collection is gorged, we need to do some trimming work.
            checkGorged(obj, true);
        }

        void insertObject(BSONObj &obj, uint64_t flags) {
            obj = addIdField(obj);
            uassert( 16328 , str::stream() << "document is larger than capped size "
                     << obj.objsize() << " > " << _maxSize, obj.objsize() <= _maxSize );

            BSONObj pk = getNextPK();
            _insertObject(pk, obj, flags | NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE, false);

            // If the collection is gorged, we need to do some trimming work.
            checkGorged(obj, false);
        }

        void deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
            massert(16460, "bug: cannot remove from a capped collection, "
                    " should have been enforced higher in the stack", false);
        }

        // run a deletion where the PK is specified
        // Can come from the applier thread on a slave
        virtual void deleteObjectFromCappedWithPK(BSONObj& pk, BSONObj& obj, uint64_t flags) {
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
        void _insertObject(BSONObj &pk, BSONObj &obj, uint64_t flags, bool checkPk) {
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
                for ( IndexCursor c(this, getPKIndex(), BSON("" << startKey), maxKey, true, 1);
                      c.ok(); c.advance() ) {
                    BSONObj oldestPK = c.currPK();
                    BSONObj oldestObj = c.current();
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
        virtual void empty() {
            SimpleMutex::scoped_lock lk(_deleteMutex);
            scoped_ptr<Cursor> c( BasicCursor::make(this) );
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

    static bool isSystemCatalog(const string &ns) {
        return str::contains(ns, ".system.indexes") || str::contains(ns, ".system.namespaces");
    }

    static bool isOplog(const string &ns) {
        return str::equals(ns.c_str(), rsoplog);
    }

    // Construct a brand new NamespaceDetails with a certain primary key and set of options.
    NamespaceDetails::NamespaceDetails(const string &ns, const BSONObj &pkIndexPattern, const BSONObj &options) :
        _ns(ns),
        _options(options.copy()),
        _pk(pkIndexPattern.copy()),
        _indexBuildInProgress(false),
        _nIndexes(0),
        _multiKeyIndexBits(0) {

        massert( 10356 ,  str::stream() << "invalid ns: " << ns , NamespaceString::validCollectionName(ns.c_str()));

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
    }
    shared_ptr<NamespaceDetails> NamespaceDetails::make(const string &ns, const BSONObj &options) {
        if (isOplog(ns)) {
            return shared_ptr<NamespaceDetails>(new OplogCollection(ns, options));
        } else if (isSystemCatalog(ns)) {
            return shared_ptr<NamespaceDetails>(new SystemCatalogCollection(ns, options));
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
        _options(serialized["options"].embeddedObject().copy()),
        _pk(serialized["pk"].embeddedObject().copy()),
        _indexBuildInProgress(false),
        _nIndexes(serialized["indexes"].Array().size()),
        _multiKeyIndexBits(static_cast<uint64_t>(serialized["multiKeyIndexBits"].Long())) {

        std::vector<BSONElement> index_array = serialized["indexes"].Array();
        for (std::vector<BSONElement>::iterator it = index_array.begin(); it != index_array.end(); it++) {
            shared_ptr<IndexDetails> idx(new IndexDetails(it->Obj(), false));
            _indexes.push_back(idx);
        }
    }
    shared_ptr<NamespaceDetails> NamespaceDetails::make(const BSONObj &serialized) {
        if (isOplog(serialized["ns"])) {
            return shared_ptr<NamespaceDetails>(new OplogCollection(serialized));
        } else if (isSystemCatalog(serialized["ns"])) {
            return shared_ptr<NamespaceDetails>(new SystemCatalogCollection(serialized));
        } else if (serialized["options"]["capped"].trueValue()) {
            return shared_ptr<NamespaceDetails>(new CappedCollection(serialized));
        } else if (serialized["options"]["natural"].trueValue()) {
            return shared_ptr<NamespaceDetails>(new NaturalOrderCollection(serialized));
        } else {
            return shared_ptr<NamespaceDetails>(new IndexedCollection(serialized));
        }
    }

    void NamespaceDetails::close() {
        for (IndexVector::iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
            IndexDetails *idx = it->get();
            idx->close();
        }
        NamespaceDetailsTransient::eraseForPrefix(_ns);
    }

    // Serialize the information necessary to re-open this NamespaceDetails later.
    BSONObj NamespaceDetails::serialize(const char *ns, const BSONObj &options, const BSONObj &pk,
            unsigned long long multiKeyIndexBits, const BSONArray &indexes_array) {
        return BSON("ns" << ns <<
                    "options" << options <<
                    "pk" << pk <<
                    "multiKeyIndexBits" << static_cast<long long>(multiKeyIndexBits) <<
                    "indexes" << indexes_array);
    }
    BSONObj NamespaceDetails::serialize() const {
        BSONArrayBuilder indexes_array;
        for (IndexVector::const_iterator it = _indexes.begin(); it != _indexes.end(); it++) {
            IndexDetails *index = it->get();
            indexes_array.append(index->info());
        }
        return serialize(_ns.c_str(), _options, _pk, _multiKeyIndexBits, indexes_array.arr());
    }

    struct findByPKCallbackExtra {
        BSONObj &obj;

        findByPKCallbackExtra(BSONObj &o) : obj(o) { }
    };

    static int findByPKCallback(const DBT *key, const DBT *value, void *extra) {
        if (key != NULL) {
            struct findByPKCallbackExtra *info = reinterpret_cast<findByPKCallbackExtra *>(extra);
            BSONObj obj(reinterpret_cast<char *>(value->data));
            info->obj = obj.getOwned();
        }
        return 0;
    }

    bool NamespaceDetails::findByPK(const BSONObj &key, BSONObj &result) const {
        int r;

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
        r = cursor->c_getf_set(cursor, 0, &key_dbt, findByPKCallback, &extra);
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

    void NamespaceDetails::insertIntoIndexes(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        dassert(!pk.isEmpty());
        dassert(!obj.isEmpty());
        if ((flags & NamespaceDetails::NO_UNIQUE_CHECKS) && _indexes.size() > 1) {
            //wunimplemented("overwrite inserts on secondary keys right now don't work");
            //uassert(16432, "can't do overwrite inserts when there are secondary keys yet", !overwrite || _indexes.size() == 1);
        }
        for (int i = 0; i < nIndexes(); i++) {
            IndexDetails &idx = *_indexes[i];
            if (i == 0) {
                dassert(isPKIndex(idx));
                idx.insertPair(pk, NULL, obj, flags);
            } else {
                BSONObjSet keys;
                idx.getKeysFromObject(obj, keys);
                if (keys.size() > 1) {
                    setIndexIsMultikey(_ns.c_str(), i);
                }
                for (BSONObjSet::const_iterator ki = keys.begin(); ki != keys.end(); ++ki) {
                    idx.insertPair(*ki, &pk, obj, flags);
                }
            }
        }
    }

    void NamespaceDetails::deleteFromIndexes(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        dassert(!pk.isEmpty());
        dassert(!obj.isEmpty());
        for (int i = 0; i < nIndexes(); i++) {
            IndexDetails &idx = *_indexes[i];

            if (i == 0) {
                idx.deletePair(pk, NULL, flags);
            } else {
                BSONObjSet keys;
                idx.getKeysFromObject(obj, keys);
                if (keys.size() > 1) {
                    dassert(isMultikey(i)); // some prior insert should have marked it as multikey
                }
                for (BSONObjSet::const_iterator ki = keys.begin(); ki != keys.end(); ++ki) {
                    idx.deletePair(*ki, &pk, flags);
                }
            }
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

        for (int i = 0; i < nIndexes(); i++) {
            IndexDetails &idx = *_indexes[i];

            if (i == 0) {
                // Overwrite oldObj with newObj using the given pk.
                idx.insertPair(pk, NULL, newObj, flags | NamespaceDetails::NO_UNIQUE_CHECKS);
            } else {
                // Determine what keys need to be removed/added.
                BSONObjSet oldKeys;
                BSONObjSet newKeys;
                idx.getKeysFromObject(oldObj, oldKeys);
                idx.getKeysFromObject(newObj, newKeys);
                if (newKeys.size() > 1) {
                    setIndexIsMultikey(_ns.c_str(), i);
                }

                // Delete the keys that exist in oldKeys but do not exist in newKeys
                for (BSONObjSet::iterator o = oldKeys.begin(); o != oldKeys.end(); o++) {
                    const BSONObj &k = *o;
                    if (!orderedSetContains(newKeys, k)) {
                        idx.deletePair(k, &pk, flags);
                    }
                }
                // Insert the keys that exist in newKeys but do not exist in oldKeys
                for (BSONObjSet::iterator n = newKeys.begin(); n != newKeys.end(); n++) {
                    const BSONObj &k = *n;
                    if (!orderedSetContains(oldKeys, k)) {
                        idx.insertPair(k, &pk, newObj, flags);
                    }
                    else if (idx.clustering()) {
                        // if clustering, overwrite every key with the new data
                        idx.insertPair(k, &pk, newObj, flags | NamespaceDetails::NO_UNIQUE_CHECKS);
                    }
                }
            }
        }
    }

    void NamespaceDetails::setIndexIsMultikey(const char *thisns, int i) {
        dassert(string(thisns) == _ns);
        dassert(i < NIndexesMax);
        unsigned long long x = ((unsigned long long) 1) << i;
        if (_multiKeyIndexBits & x) {
            return;
        }
        if (!Lock::isWriteLocked(_ns)) {
            throw RetryWithWriteLock();
        }
        _multiKeyIndexBits |= x;

        dassert(nsdetails(thisns) == this);
        nsindex(thisns)->update_ns(thisns, serialize(), true);

        NamespaceDetailsTransient::get(thisns).clearQueryCache();
    }

    void NamespaceDetails::buildIndex(shared_ptr<IndexDetails> &index) {
        _indexBuildInProgress = true;

        IndexDetails::Builder builder(*index);

        const int indexNum = idxNo(*index);
        for ( scoped_ptr<Cursor> cursor(BasicCursor::make(this));
              cursor->ok(); cursor->advance()) {
            BSONObj pk = cursor->currPK();
            BSONObj obj = cursor->current();
            BSONObjSet keys;
            index->getKeysFromObject(obj, keys);
            if (keys.size() > 1) {
                setIndexIsMultikey(_ns.c_str(), indexNum);
            }
            for (BSONObjSet::const_iterator ki = keys.begin(); ki != keys.end(); ++ki) {
                builder.insertPair(*ki, &pk, obj);
            }
            killCurrentOp.checkForInterrupt(false); // uasserts if we should stop
        }

        builder.done();

        // If the index is unique, check all adjacent keys for a duplicate.
        if (index->unique()) {
            IndexScanCursor c(this, *index, 1);
            BSONObj prevKey = c.currKey().getOwned();
            c.advance();
            for ( ; c.ok(); c.advance()) {
                BSONObj currKey = c.currKey(); 
                if (currKey == prevKey) {
                    index->uassertedDupKey(currKey);
                }
                prevKey = currKey.getOwned();
            }
        }

        _indexBuildInProgress = false;
    }

    void NamespaceDetails::createIndex(const BSONObj &idx_info) {
        uassert(16449, "dropDups is not supported and is likely to remain unsupported for some time because it deletes arbitrary data",
                !idx_info["dropDups"].trueValue());
        uassert(12588, "cannot add index with a background operation in progress", !_indexBuildInProgress);
        uassert(12523, "no index name specified", idx_info["name"].ok());

        const string &name = idx_info["name"].String();
        if (findIndexByName(name.c_str()) >= 0) {
            // index already exists.
            uasserted(16753, mongoutils::str::stream() << "index with name " << name << " already exists");
        }
        const BSONObj &keyPattern = idx_info["key"].Obj();
        if (findIndexByKeyPattern(keyPattern) >= 0) {
            string s = (mongoutils::str::stream() << "index already exists with diff name " << name << ' ' << keyPattern.toString());
            LOG(2) << s << endl;
            uasserted(16754, s);
        }

        if (nIndexes() >= NIndexesMax ) {
            // calling BSONObj::str() (with NOINLINE) ensures the symbol exists in gdb.
            // don't modify that line without putting a call to str() elsewhere.
            string s = (mongoutils::str::stream() <<
                        "add index fails, too many indexes for " << name <<
                        " key:" << keyPattern.toString());
            log() << s << endl;
            uasserted(12505,s);
        }

        if (!Lock::isWriteLocked(_ns)) {
            throw RetryWithWriteLock();
        }

        // Note this ns in the rollback so if this transaction aborts, we'll
        // close this ns, forcing the next user to reload in-memory metadata.
        NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
        rollback.noteNs(_ns.c_str());

        shared_ptr<IndexDetails> index(new IndexDetails(idx_info));
        // Ensure we initialize the spec in case the collection is empty.
        // This also causes an error to be thrown if we're trying to create an invalid index on an empty collection.
        const bool isSecondaryIndex = _nIndexes > 0;
        try {
            index->getSpec();
            _indexes.push_back(index);

            // Only secondary indexes need to be built.
            if (isSecondaryIndex) {
                try {
                    buildIndex(index);
                }
                catch (...) {
                    _indexes.pop_back();
                    throw;
                }
            }
        }
        catch (...) {
            // Can't let the IndexDetails destructor get called on its own any more, see IndexDetails::close for why.
            index->close();
            throw;
        }
        _nIndexes++;

        string idx_ns = idx_info["ns"].String();
        const char *ns = idx_ns.c_str();

        // The first index we create should be the pk index, when we first create the collection.
        // Therefore the collection's NamespaceDetails should not already exist in the NamespaceIndex
        // unless we are building a secondary index (and therefore the collection already exists)
        const bool may_overwrite = isSecondaryIndex;
        if (!may_overwrite) {
            massert(16435, "first index should be pk index", index->keyPattern() == _pk);
        }
        nsindex(ns)->update_ns(ns, serialize(), isSecondaryIndex);

        NamespaceDetailsTransient::get(ns).addedIndex();
    }

    // Normally, we cannot drop the _id_ index.
    // The parameters mayDeleteIdIndex is here for the case where we call dropIndexes
    // through dropCollection, in which case we are dropping an entire collection,
    // hence the _id_ index will have to go.
    bool NamespaceDetails::dropIndexes(const char *ns, const char *name, string &errmsg, BSONObjBuilder &result, bool mayDeleteIdIndex) {
        Lock::assertWriteLocked(ns);
        TOKULOG(1) << "dropIndexes " << name << endl;

        // Note this ns in the rollback so if this transaction aborts, we'll
        // close this ns, forcing the next user to reload in-memory metadata.
        NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
        rollback.noteNs(_ns.c_str());

        NamespaceDetails *d = nsdetails(ns);
        ClientCursor::invalidate(ns);

        if (mongoutils::str::equals(name, "*")) {
            result.append("nIndexesWas", (double) _nIndexes);
            // This is O(n^2), not great, but you can have at most 64 indexes anyway.
            for (IndexVector::iterator it = _indexes.begin(); it != _indexes.end(); ) {
                IndexDetails *idx = it->get();
                if (mayDeleteIdIndex || (!idx->isIdIndex() && !d->isPKIndex(*idx))) {
                    idx->kill_idx();
                    it = _indexes.erase(it);
                    _nIndexes--;
                } else {
                    it++;
                }
            }
            // Assuming id index isn't multikey
            _multiKeyIndexBits = 0;
            result.append("msg", (mayDeleteIdIndex
                                  ? "indexes dropped for collection"
                                  : "non-_id indexes dropped for collection"));
        } else {
            int x = findIndexByName(name);
            if (x >= 0) {
                result.append("nIndexesWas", (double) _nIndexes);
                IndexVector::iterator it = _indexes.begin() + x;
                IndexDetails *idx = it->get();
                if ( !mayDeleteIdIndex && (idx->isIdIndex() || d->isPKIndex(*idx)) ) {
                    errmsg = "may not delete _id or $_ index";
                    return false;
                }
                idx->kill_idx();
                _indexes.erase(it);
                _nIndexes--;
                // Removes the nth bit, and shifts any bits higher than it down a slot.
                _multiKeyIndexBits = ((_multiKeyIndexBits & ((1ULL << x) - 1)) |
                                     ((_multiKeyIndexBits >> (x + 1)) << x));
            } else {
                // theoretically, this should not be needed, as we do all of our fileops
                // transactionally, but keeping this here just in case at the moment
                // just in case an orphaned listing there - i.e. should have been repaired but wasn't
                int n = removeFromSysIndexes(ns, name);
                if (n) {
                    log() << "info: removeFromSysIndexes cleaned up " << n << " entries" << endl;
                }
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
        for (IndexVector::const_iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
            IndexDetails *index = it->get();
            IndexStats stats(*index);
            indexStats.push_back(stats);
        }
    }

    void NamespaceDetails::optimize() {
        for (IndexVector::const_iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
            IndexDetails *index = it->get();
            index->optimize();
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
        string indexns = info["ns"].String();
        if (mongoutils::str::contains(indexns, ".system.indexes")) {
            // system.indexes holds all the others, so it is not explicitly listed in the catalog.
            return;
        }

        char database[256];
        nsToDatabase(indexns.c_str(), database);
        string s = string(database) + ".system.indexes";
        const char *ns = s.c_str();
        NamespaceDetails *d = nsdetails_maybe_create(ns);
        NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get(ns);
        BSONObj objMod = info;
        insertOneObject(d, nsdt, objMod);
    }

    void NamespaceDetails::addDefaultIndexesToCatalog() {
        // Either a single primary key or a hidden primary key + _id index.
        dassert(_nIndexes == 1 || (_nIndexes == 2 && findIdIndex() == 1));
        for (int i = 0; i < nIndexes(); i++) {
            addIndexToCatalog(_indexes[i]->info());
        }
    }

    /* ------------------------------------------------------------------------- */

    SimpleMutex NamespaceDetailsTransient::_qcMutex("qc");
    SimpleMutex NamespaceDetailsTransient::_isMutex("is");
    map< string, shared_ptr< NamespaceDetailsTransient > > NamespaceDetailsTransient::_nsdMap;
    typedef map< string, shared_ptr< NamespaceDetailsTransient > >::iterator ouriter;

    void NamespaceDetailsTransient::reset() {
        // TODO(leif): why is this here?
        //Lock::assertWriteLocked(_ns); 
        clearQueryCache();
        _keysComputed = false;
        _indexSpecs.clear();
    }

    /*static*/ NOINLINE_DECL NamespaceDetailsTransient& NamespaceDetailsTransient::make_inlock(const char *ns) {
        shared_ptr< NamespaceDetailsTransient > &t = _nsdMap[ ns ];
        verify( t.get() == 0 );
        if( _nsdMap.size() % 20000 == 10000 ) { 
            // so we notice if insanely large #s
            log() << "opening namespace " << ns << endl;
            log() << _nsdMap.size() << " namespaces in nsdMap" << endl;
        }
        t.reset( new NamespaceDetailsTransient(ns) );
        return *t;
    }

    NamespaceDetailsTransient::NamespaceDetailsTransient(const char *ns) : 
        _ns(ns), _keysComputed(false), _qcWriteCount() {
    }

    NamespaceDetailsTransient::~NamespaceDetailsTransient() { 
    }

    void NamespaceDetailsTransient::clearForPrefix(const char *prefix) {
        SimpleMutex::scoped_lock lk(_qcMutex);
        vector< string > found;
        for( ouriter i = _nsdMap.begin(); i != _nsdMap.end(); ++i ) {
            if ( strncmp( i->first.c_str(), prefix, strlen( prefix ) ) == 0 ) {
                found.push_back( i->first );
                Lock::assertWriteLocked(i->first);
            }
        }
        for( vector< string >::iterator i = found.begin(); i != found.end(); ++i ) {
            _nsdMap[ *i ].reset();
        }
    }

    void NamespaceDetailsTransient::eraseForPrefix(const char *prefix) {
        SimpleMutex::scoped_lock lk(_qcMutex);
        vector< string > found;
        for( ouriter i = _nsdMap.begin(); i != _nsdMap.end(); ++i ) {
            if ( strncmp( i->first.c_str(), prefix, strlen( prefix ) ) == 0 ) {
                found.push_back( i->first );
                Lock::assertWriteLocked(i->first);
            }
        }
        for( vector< string >::iterator i = found.begin(); i != found.end(); ++i ) {
            _nsdMap.erase(*i);
        }
    }

    void NamespaceDetailsTransient::computeIndexKeys() {
        _indexKeys.clear();
        NamespaceDetails *d = nsdetails(_ns.c_str());
        if ( ! d )
            return;
        NamespaceDetails::IndexIterator i = d->ii();
        while( i.more() )
            i.next().keyPattern().getFieldNames(_indexKeys);
        _keysComputed = true;
    }

    // TODO: All global functions manipulating namespaces should be static in NamespaceDetails

    static void checkConfigNS(const char *ns) {
        if ( cmdLine.configsvr &&
             !( mongoutils::str::startsWith( ns, "config." ) ||
                mongoutils::str::startsWith( ns, "local." ) ||
                mongoutils::str::startsWith( ns, "admin." ) ) ) {
            uasserted(14037, "can't create user databases on a --configsvr instance");
        }
    }

    bool userCreateNS(const char *ns, BSONObj options, string& err, bool logForReplication) {
        const char *coll = strchr( ns, '.' ) + 1;
        massert( 16451 ,  str::stream() << "invalid ns: " << ns , NamespaceString::validCollectionName(ns));
        char cl[ 256 ];
        nsToDatabase( ns, cl );
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
            string logNs = string( cl ) + ".$cmd";
            OpLogHelpers::logCommand(logNs.c_str(), options, &cc().txn());
        }
        // TODO: Identify error paths for this function
        return true;
    }

    NamespaceDetails* getAndMaybeCreateNS(const char *ns, bool logop) {
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

    void dropDatabase(const string &name) {
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

        nsindex(name.c_str())->drop();
        Database::closeDatabase(d->name().c_str(), d->path());
    }

    void dropCollection(const string &name, string &errmsg, BSONObjBuilder &result, bool can_drop_system) {
        TOKULOG(1) << "dropCollection " << name << endl;
        const char *ns = name.c_str();
        NamespaceDetails *d = nsdetails(ns);
        if (d == NULL) {
            return;
        }

        // Check that we are allowed to drop the namespace.
        NamespaceString s(name);
        verify(s.db == cc().database()->name());
        if (s.isSystem()) {
            if (s.coll == "system.profile") {
                uassert(10087, "turn off profiling before dropping system.profile collection", cc().database()->profile() == 0);
            } else if (!can_drop_system) {
                uasserted(12502, "can't drop system ns");
            }
        }

        // Invalidate cursors and query cache, then drop all of the indexes.
        ClientCursor::invalidate(ns);
        NamespaceDetailsTransient::eraseForPrefix(ns);

        LOG(1) << "\t dropIndexes done" << endl;
        d->dropIndexes(ns, "*", errmsg, result, true);
        verify(d->nIndexes() == 0);
        removeNamespaceFromCatalog(name);

        // If everything succeeds, kill the namespace from the nsindex.
        Top::global.collectionDropped(name);
        nsindex(ns)->kill_ns(ns);
        result.append("ns", name);
    }

    /* add a new namespace to the system catalog (<dbname>.system.namespaces).
       options: { capped : ..., size : ... }
    */
    void addNewNamespaceToCatalog(const string &ns, const BSONObj *options) {
        LOG(1) << "New namespace: " << ns << endl;
        if (mongoutils::str::contains(ns, ".system.namespaces") ) {
            // system.namespaces holds all the others, so it is not explicitly listed in the catalog.
            return;
        }

        BSONObjBuilder b;
        b.append("name", ns);
        if ( options ) {
            b.append("options", *options);
        }
        BSONObj info = b.done();

        char database[256];
        nsToDatabase(ns.c_str(), database);
        string s = string(database) + ".system.namespaces";
        const char *system_ns = s.c_str();
        NamespaceDetails *d = nsdetails_maybe_create(system_ns);
        NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get(system_ns);
        insertOneObject(d, nsdt, info);
    }

    void removeNamespaceFromCatalog(const string &ns) {
        if (!mongoutils::str::contains(ns, ".system.namespaces")) {
            string system_namespaces = cc().database()->name() + ".system.namespaces";
            _deleteObjects(system_namespaces.c_str(), BSON("name" << ns), false, false);
        }
    }

    int removeFromSysIndexes(const char *ns, const char *name) {
        string system_indexes = cc().database()->name() + ".system.indexes";
        BSONObj obj = BSON("ns" << ns << "name" << name);
        TOKULOG(2) << "removeFromSysIndexes removing " << obj << endl;
        return (int) _deleteObjects(system_indexes.c_str(), obj, false, false);
    }

    static BSONObj replaceNSField(const BSONObj &obj, const char *to) {
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

    void renameNamespace(const char *from, const char *to, bool stayTemp) {
        Lock::assertWriteLocked(from);
        verify( nsdetails(from) != NULL );
        verify( nsdetails(to) == NULL );

        // Invalidate any existing cursors on the old namespace details,
        // and reset the query cache.
        ClientCursor::invalidate( from );
        NamespaceDetailsTransient::eraseForPrefix( from );

        char database[MaxDatabaseNameLen];
        nsToDatabase(from, database);
        string sysIndexes = string(database) + ".system.indexes";
        string sysNamespaces = string(database) + ".system.namespaces";

        // Generate the serialized form of the namespace, and then close it.
        // This will close the underlying dictionaries and allow us to
        // rename them in the environment.
        BSONObj serialized = nsdetails(from)->serialize();
        bool closed = nsindex(from)->close_ns(from);
        verify(closed);

        // Rename each index in system.indexes and system.namespaces
        {
            BSONObj nsQuery = BSON( "ns" << from );
            vector<BSONObj> indexSpecs = Helpers::findAll( sysIndexes.c_str(), nsQuery );
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
            verify( Helpers::findOne( sysNamespaces.c_str(), BSON( "name" << from ), oldSpec ) );
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

    bool legalClientSystemNS( const string& ns , bool write ) {
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
