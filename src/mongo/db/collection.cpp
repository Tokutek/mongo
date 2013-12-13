/**
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

#include "mongo/base/init.h"
#include "mongo/db/collection.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/index.h"
#include "mongo/db/index_set.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/repl/rs.h"

namespace mongo {

    static BSONObj addIdField(const BSONObj &obj) {
        if (obj.hasField("_id")) {
            return obj;
        } else {
            // _id first, everything else after
            BSONObjBuilder b;
            OID oid;
            oid.init();
            b.append("_id", oid);
            b.appendElements(obj);
            return b.obj();
        }
    }

    static BSONObj inheritIdField(const BSONObj &oldObj, const BSONObj &newObj) {
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

    // ------------------------------------------------------------------------

    BSONObj IndexedCollection::determinePrimaryKey(const BSONObj &options) {
        const BSONObj idPattern = BSON("_id" << 1);
        BSONObj pkPattern = idPattern;
        if (options["primaryKey"].ok()) {
            uassert(17209, "defined primary key must be an object", options["primaryKey"].type() == Object);
            pkPattern = options["primaryKey"].Obj();
            bool pkPatternLast = false;
            for (BSONObjIterator i(pkPattern); i.more(); ) {
                const BSONElement e = i.next();
                if (!i.more()) {
                    // This is the last element. Needs to be _id: 1
                    pkPatternLast = e.wrap() == idPattern;
                }
            }
            uassert(17203, "defined primary key must end in _id: 1", pkPatternLast);
            uassert(17204, "defined primary key cannot be sparse", !options["sparse"].trueValue());
        }
        return pkPattern;
    }

    IndexedCollection::IndexedCollection(const StringData &ns, const BSONObj &options) :
        NamespaceDetails(ns, determinePrimaryKey(options), options),
        // determinePrimaryKey() was called, so whatever the pk is, it
        // exists in _indexes. Thus, we know we have an _id primary key
        // if we can find an index with pattern "_id: 1" at this point.
        _idPrimaryKey(findIndexByKeyPattern(BSON("_id" << 1)) >= 0) {
        const int idxNo = findIndexByKeyPattern(BSON("_id" << 1));
        if (idxNo < 0) {
            // create a unique, non-clustering _id index here.
            BSONObj info = indexInfo(BSON("_id" << 1), true, false);
            createIndex(info);
        }
        verify(_idPrimaryKey == idx(findIndexByKeyPattern(BSON("_id" << 1))).clustering());
    }

    IndexedCollection::IndexedCollection(const BSONObj &serialized) :
        NamespaceDetails(serialized),
        _idPrimaryKey(idx(findIndexByKeyPattern(BSON("_id" << 1))).clustering()) {
    }

    // inserts an object into this namespace, taking care of secondary indexes if they exist
    void IndexedCollection::insertObject(BSONObj &obj, uint64_t flags) {
        obj = addIdField(obj);
        const BSONObj pk = getValidatedPKFromObject(obj);

        // We skip unique checks if the primary key is something other than the _id index.
        // Any other PK is guaranteed to contain the _id somewhere in its pattern, so
        // we know that PK is unique since a unique key on _id must exist.
        insertIntoIndexes(pk, obj, flags | (!_idPrimaryKey ? NO_PK_UNIQUE_CHECKS : 0));
    }

    void IndexedCollection::updateObject(const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj,
                                         const bool logop, const bool fromMigrate,
                                         uint64_t flags) {
        const BSONObj newObjWithId = inheritIdField(oldObj, newObj);

        if (_idPrimaryKey) {
            NamespaceDetails::updateObject(pk, oldObj, newObjWithId, logop, fromMigrate, flags | NO_PK_UNIQUE_CHECKS);
        } else {
            const BSONObj newPK = getValidatedPKFromObject(newObjWithId);
            dassert(newPK.nFields() == pk.nFields());
            if (newPK != pk) {
                // Primary key has changed - that means all indexes will be affected.
                deleteFromIndexes(pk, oldObj, flags);
                insertIntoIndexes(newPK, newObjWithId, flags);
                if (logop) {
                    OpLogHelpers::logDelete(_ns.c_str(), oldObj, fromMigrate);
                    OpLogHelpers::logInsert(_ns.c_str(), newObjWithId);
                }
            } else {
                // Skip unique checks on the primary key - we know it did not change.
                NamespaceDetails::updateObject(pk, oldObj, newObjWithId, logop, fromMigrate, flags | NO_PK_UNIQUE_CHECKS);
            }
        }
    }

    // Overridden to optimize the case where we have an _id primary key.
    BSONObj IndexedCollection::getValidatedPKFromObject(const BSONObj &obj) const {
        if (_idPrimaryKey) {
            const BSONElement &e = obj["_id"];
            dassert(e.ok() && e.type() != Array &&
                    e.type() != RegEx && e.type() != Undefined); // already checked in ops/insert.cpp
            return e.wrap("");
        } else {
            return NamespaceDetails::getValidatedPKFromObject(obj);
        }
    }

    // Overriden to optimize pk generation for an _id primary key.
    // We just need to look for the _id field and, if it exists
    // and is simple, return a wrapped object.
    BSONObj IndexedCollection::getSimplePKFromQuery(const BSONObj &query) const {
        if (_idPrimaryKey) {
            const BSONElement &e = query["_id"];
            if (e.ok() && e.isSimpleType() &&
                !(e.type() == Object && e.Obj().firstElementFieldName()[0] == '$')) {
                return e.wrap("");
            }
            return BSONObj();
        } else {
            return NamespaceDetails::getSimplePKFromQuery(query);
        }
    }

    // ------------------------------------------------------------------------

    OplogCollection::OplogCollection(const StringData &ns, const BSONObj &options) :
        IndexedCollection(ns, options) {
        uassert(17206, "must not define a primary key for the oplog",
                       !options["primaryKey"].ok());
    } 

    OplogCollection::OplogCollection(const BSONObj &serialized) :
        IndexedCollection(serialized) {
    }

    BSONObj OplogCollection::minUnsafeKey() {
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

    // ------------------------------------------------------------------------

    NaturalOrderCollection::NaturalOrderCollection(const StringData &ns, const BSONObj &options) :
        NamespaceDetails(ns, BSON("$_" << 1), options),
        _nextPK(0) {
    }

    NaturalOrderCollection::NaturalOrderCollection(const BSONObj &serialized) :
        NamespaceDetails(serialized),
        _nextPK(0) {
        Client::Transaction txn(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
        {
            // The next PK, if it exists, is the last pk + 1
            shared_ptr<Cursor> cursor = BasicCursor::make(this, -1);
            if (cursor->ok()) {
                const BSONObj key = cursor->currPK();
                dassert(key.nFields() == 1);
                _nextPK = AtomicWord<long long>(key.firstElement().Long() + 1);
            }
        }
        txn.commit();
    }

    // insert an object, using a fresh auto-increment primary key
    void NaturalOrderCollection::insertObject(BSONObj &obj, uint64_t flags) {
        BSONObjBuilder pk(64);
        pk.append("", _nextPK.fetchAndAdd(1));
        insertIntoIndexes(pk.obj(), obj, flags);
    }

    // ------------------------------------------------------------------------

    SystemCatalogCollection::SystemCatalogCollection(const StringData &ns, const BSONObj &options) :
        NaturalOrderCollection(ns, options) {
    }

    SystemCatalogCollection::SystemCatalogCollection(const BSONObj &serialized) :
        NaturalOrderCollection(serialized) {
    }

    void SystemCatalogCollection::insertObject(BSONObj &obj, uint64_t flags) {
        obj = beautify(obj);
        NaturalOrderCollection::insertObject(obj, flags);
    }

    void SystemCatalogCollection::createIndex(const BSONObj &info) {
        msgasserted(16464, "bug: system collections should not be indexed." );
    }

    // For consistency with Vanilla MongoDB, the system catalogs have the following
    // fields, in order, if they exist.
    //
    //  { key, unique, ns, name, [everything else] }
    //
    // This code is largely borrowed from prepareToBuildIndex() in Vanilla.
    BSONObj SystemCatalogCollection::beautify(const BSONObj &obj) {
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

    // ------------------------------------------------------------------------

    BSONObj oldSystemUsersKeyPattern;
    BSONObj extendedSystemUsersKeyPattern;
    std::string extendedSystemUsersIndexName;
    namespace {
        MONGO_INITIALIZER(AuthIndexKeyPatterns)(InitializerContext*) {
            oldSystemUsersKeyPattern = BSON(AuthorizationManager::USER_NAME_FIELD_NAME << 1);
            extendedSystemUsersKeyPattern = BSON(AuthorizationManager::USER_NAME_FIELD_NAME << 1 <<
                                                 AuthorizationManager::USER_SOURCE_FIELD_NAME << 1);
            extendedSystemUsersIndexName = std::string(str::stream() <<
                                                       AuthorizationManager::USER_NAME_FIELD_NAME <<
                                                       "_1_" <<
                                                       AuthorizationManager::USER_SOURCE_FIELD_NAME <<
                                                       "_1");
            return Status::OK();
        }
    }

    BSONObj SystemUsersCollection::extendedSystemUsersIndexInfo(const StringData &ns) {
        BSONObjBuilder indexBuilder;
        indexBuilder.append("key", extendedSystemUsersKeyPattern);
        indexBuilder.appendBool("unique", true);
        indexBuilder.append("ns", ns);
        indexBuilder.append("name", extendedSystemUsersIndexName);
        return indexBuilder.obj();
    }

    SystemUsersCollection::SystemUsersCollection(const StringData &ns, const BSONObj &options) :
        IndexedCollection(ns, options) {
        BSONObj info = extendedSystemUsersIndexInfo(ns);
        createIndex(info);
        uassert(17207, "must not define a primary key for the system.users collection",
                       !options["primaryKey"].ok());
    }

    SystemUsersCollection::SystemUsersCollection(const BSONObj &serialized) :
        IndexedCollection(serialized) {
        int idx = findIndexByKeyPattern(extendedSystemUsersKeyPattern);
        if (idx < 0) {
            BSONObj info = extendedSystemUsersIndexInfo(_ns);
            createIndex(info);
            addToIndexesCatalog(info);
        }
        idx = findIndexByKeyPattern(oldSystemUsersKeyPattern);
        if (idx >= 0) {
            dropIndex(idx);
        }
    }

    // ------------------------------------------------------------------------

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
    CappedCollection::CappedCollection(const StringData &ns, const BSONObj &options,
                                       const bool mayIndexId) :
        NaturalOrderCollection(ns, options),
        _maxSize(BytesQuantity<long long>(options["size"])),
        _maxObjects(BytesQuantity<long long>(options["max"])),
        _currentObjects(0),
        _currentSize(0),
        _mutex("cappedMutex"),
        _deleteMutex("cappedDeleteMutex") {

        // Create an _id index if "autoIndexId" is missing or it exists as true.
        if (mayIndexId) {
            const BSONElement e = options["autoIndexId"];
            if (!e.ok() || e.trueValue()) {
                BSONObj info = indexInfo(BSON("_id" << 1), true, false);
                createIndex(info);
            }
        }
    }
    CappedCollection::CappedCollection(const BSONObj &serialized) :
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

    void CappedCollection::fillSpecificStats(BSONObjBuilder &result, int scale) const {
        result.appendBool("capped", true);
        if (_maxObjects) {
            result.appendNumber("max", _maxObjects);
        }
        result.appendNumber("cappedCount", _currentObjects.load());
        result.appendNumber("cappedSizeMax", _maxSize);
        result.appendNumber("cappedSizeCurrent", _currentSize.load());
    }

    bool CappedCollection::isCapped() const {
        dassert(_options["capped"].trueValue());
        return true;
    }

    // @return the maximum safe key to read for a tailable cursor.
    BSONObj CappedCollection::minUnsafeKey() {
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
    void CappedCollection::insertObjectIntoCappedWithPK(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
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

    void CappedCollection::insertObjectIntoCappedAndLogOps(const BSONObj &obj, uint64_t flags) {
        const BSONObj objWithId = addIdField(obj);
        uassert( 16774 , str::stream() << "document is larger than capped size "
                 << objWithId.objsize() << " > " << _maxSize, objWithId.objsize() <= _maxSize );

        const BSONObj pk = getNextPK();
        checkUniqueAndInsert(pk, objWithId, flags | NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE, false);
        OpLogHelpers::logInsertForCapped(_ns.c_str(), pk, objWithId);
        checkGorged(obj, true);
    }

    void CappedCollection::insertObject(BSONObj &obj, uint64_t flags) {
        obj = addIdField(obj);
        _insertObject(obj, flags);
    }

    void CappedCollection::deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        msgasserted(16460, "bug: cannot remove from a capped collection, "
                           " should have been enforced higher in the stack" );
    }

    // run a deletion where the PK is specified
    // Can come from the applier thread on a slave
    void CappedCollection::deleteObjectFromCappedWithPK(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        _deleteObject(pk, obj, flags);
        // just make it easy and invalidate this
        _lastDeletedPK = BSONObj();
    }

    void CappedCollection::updateObject(const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj,
                      const bool logop, const bool fromMigrate,
                      uint64_t flags) {
        const BSONObj newObjWithId = inheritIdField(oldObj, newObj);
        long long diff = newObjWithId.objsize() - oldObj.objsize();
        uassert( 10003 , "failing update: objects in a capped ns cannot grow", diff <= 0 );

        NamespaceDetails::updateObject(pk, oldObj, newObjWithId, logop, fromMigrate, flags);
        if (diff < 0) {
            _currentSize.addAndFetch(diff);
        }
    }

    void CappedCollection::updateObjectMods(const BSONObj &pk, const BSONObj &updateobj,
                          const bool logop, const bool fromMigrate,
                          uint64_t flags) {
        msgasserted(17217, "bug: cannot (fast) update a capped collection, "
                           " should have been enforced higher in the stack" );
    }

    void CappedCollection::_insertObject(const BSONObj &obj, uint64_t flags) {
        uassert( 16328 , str::stream() << "document is larger than capped size "
                 << obj.objsize() << " > " << _maxSize, obj.objsize() <= _maxSize );

        const BSONObj pk = getNextPK();
        checkUniqueAndInsert(pk, obj, flags | NamespaceDetails::NO_UNIQUE_CHECKS | NamespaceDetails::NO_LOCKTREE, false);
        checkGorged(obj, false);
    }

    // Note the commit of a transaction, which simple notes completion under the lock.
    // We don't need to do anything with nDelta and sizeDelta because those changes
    // are already applied to in-memory stats, and this transaction has committed.
    void CappedCollection::noteCommit(const BSONObj &minPK, long long nDelta, long long sizeDelta) {
        noteComplete(minPK);
    }

    // Note the abort of a transaction, noting completion and updating in-memory stats.
    //
    // The given deltas are signed values that represent changes to the collection.
    // We need to roll back those changes. Therefore, we subtract from the current value.
    void CappedCollection::noteAbort(const BSONObj &minPK, long long nDelta, long long sizeDelta) {
        noteComplete(minPK);
        _currentObjects.fetchAndSubtract(nDelta);
        _currentSize.fetchAndSubtract(sizeDelta);

        // If this transaction did inserts, it probably did deletes to make room
        // for the new objects. Invalidate the last key deleted so that new
        // trimming work properly recognizes that our deletes have been aborted.
        SimpleMutex::scoped_lock lk(_deleteMutex);
        _lastDeletedPK = BSONObj();
    }

    // requires: _mutex is held
    void CappedCollection::noteUncommittedPK(const BSONObj &pk) {
        CappedCollectionRollback &rollback = cc().txn().cappedRollback();
        if (!rollback.hasNotedInsert(_ns)) {
            // This transaction has not noted an insert yet, so we save this
            // as a minimum uncommitted PK. The next insert by this txn won't be
            // the minimum, and rollback.hasNotedInsert() will be true, so
            // we won't save it.
            _uncommittedMinPKs.insert(pk.getOwned());
        }
    }

    BSONObj CappedCollection::getNextPK() {
        SimpleMutex::scoped_lock lk(_mutex);
        BSONObjBuilder b(32);
        b.append("", _nextPK.fetchAndAdd(1));
        BSONObj pk = b.obj();
        noteUncommittedPK(pk);
        return pk;
    }

    // Note the completion of a transaction by removing its
    // minimum-PK-inserted (if there is one) from the set.
    void CappedCollection::noteComplete(const BSONObj &minPK) {
        if (!minPK.isEmpty()) {
            SimpleMutex::scoped_lock lk(_mutex);
            const int n = _uncommittedMinPKs.erase(minPK);
            verify(n == 1);
        }
    }

    void CappedCollection::checkGorged(const BSONObj &obj, bool logop) {
        // If the collection is gorged, we need to do some trimming work.
        long long n = _currentObjects.load();
        long long size = _currentSize.load();
        if (isGorged(n, size)) {
            trim(obj.objsize(), logop);
        }
    }

    void CappedCollection::checkUniqueIndexes(const BSONObj &pk, const BSONObj &obj, bool checkPk) {
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
                    idx.uniqueCheck(*ki, pk);
                }
            }
        }
    }

    // Checks unique indexes and does the actual inserts.
    // Does not check if the collection became gorged.
    void CappedCollection::checkUniqueAndInsert(const BSONObj &pk, const BSONObj &obj, uint64_t flags, bool checkPk) {
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

    bool CappedCollection::isGorged(long long n, long long size) const {
        return (_maxObjects > 0 && n > _maxObjects) || (_maxSize > 0 && size > _maxSize);
    }

    void CappedCollection::_deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        // Note the delete we're about to do.
        size_t size = obj.objsize();
        CappedCollectionRollback &rollback = cc().txn().cappedRollback();
        rollback.noteDelete(_ns, pk, size);
        _currentObjects.subtractAndFetch(1);
        _currentSize.subtractAndFetch(size);

        NaturalOrderCollection::deleteObject(pk, obj, flags);
    }

    void CappedCollection::trim(int objsize, bool logop) {
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
            for (shared_ptr<Cursor> c(IndexCursor::make(this, getPKIndex(),
                                      BSON("" << startKey), maxKey, true, 1));
                 c->ok(); c->advance()) {
                BSONObj oldestPK = c->currPK();
                BSONObj oldestObj = c->current();
                trimmedBytes += oldestPK.objsize();
                
                if (logop) {
                    OpLogHelpers::logDeleteForCapped(_ns.c_str(), oldestPK, oldestObj);
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
    void CappedCollection::empty() {
        SimpleMutex::scoped_lock lk(_deleteMutex);
        for (shared_ptr<Cursor> c( BasicCursor::make(this) ); c->ok() ; c->advance()) {
            _deleteObject(c->currPK(), c->current(), 0);
        }
        _lastDeletedPK = BSONObj();
    }

    // ------------------------------------------------------------------------

    ProfileCollection::ProfileCollection(const StringData &ns, const BSONObj &options) :
        // Never automatically index the _id field
        CappedCollection(ns, options, false) {
    }

    ProfileCollection::ProfileCollection(const BSONObj &serialized) :
        CappedCollection(serialized) {
    }

    void ProfileCollection::insertObjectIntoCappedWithPK(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        msgasserted( 16847, "bug: The profile collection should not have replicated inserts." );
    }

    void ProfileCollection::insertObjectIntoCappedAndLogOps(const BSONObj &obj, uint64_t flags) {
        msgasserted( 16848, "bug: The profile collection should not not log inserts." );
    }

    void ProfileCollection::insertObject(BSONObj &obj, uint64_t flags) {
        _insertObject(obj, flags);
    }

    void ProfileCollection::deleteObjectFromCappedWithPK(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        msgasserted( 16849, "bug: The profile collection should not have replicated deletes." );
    }

    void ProfileCollection::updateObject(const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj,
                      const bool logop, const bool fromMigrate,
                      uint64_t flags) {
        msgasserted( 16850, "bug: The profile collection should not be updated." );
    }

    void ProfileCollection::updateObjectMods(const BSONObj &pk, const BSONObj &updateobj,
                      const bool logop, const bool fromMigrate,
                      uint64_t flags) {
        msgasserted( 17219, "bug: The profile collection should not be updated." );
    }

    void ProfileCollection::createIndex(const BSONObj &idx_info) {
        uassert(16851, "Cannot have an _id index on the system profile collection", !idx_info["key"]["_id"].ok());
    }

    // ------------------------------------------------------------------------

    BulkLoadedCollection::BulkLoadedCollection(const BSONObj &serialized) :
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

    void BulkLoadedCollection::close(const bool abortingLoad) {
        class FinallyClose : boost::noncopyable {
        public:
            FinallyClose(BulkLoadedCollection &coll) : c(coll) {}
            ~FinallyClose() {
                c._close();
            }
        private:
            BulkLoadedCollection &c;
        } finallyClose(*this);

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
    }

    void BulkLoadedCollection::validateConnectionId(const ConnectionId &id) {
        uassert( 16878, str::stream() << "This connection cannot use ns " << _ns <<
                        ", it is currently under-going bulk load by connection id "
                        << _bulkLoadConnectionId,
                        _bulkLoadConnectionId == id );
    }

    void BulkLoadedCollection::insertObject(BSONObj &obj, uint64_t flags) {
        obj = addIdField(obj);
        const BSONObj pk = getValidatedPKFromObject(obj);

        storage::Key sPK(pk, NULL);
        DBT key = storage::dbt_make(sPK.buf(), sPK.size());
        DBT val = storage::dbt_make(obj.objdata(), obj.objsize());
        _loader->put(&key, &val);
    }

    void BulkLoadedCollection::deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        uasserted( 16865, "Cannot delete from a collection under-going bulk load." );
    }

    void BulkLoadedCollection::updateObject(const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj,
                                            const bool logop, const bool fromMigrate,
                                            uint64_t flags) {
        uasserted( 16866, "Cannot update a collection under-going bulk load." );
    }

    void BulkLoadedCollection::updateObjectMods(const BSONObj &pk, const BSONObj &updateobj,
                                                const bool logop, const bool fromMigrate,
                                                uint64_t flags) {
        uasserted( 17218, "Cannot update a collection under-going bulk load." );
    }

    void BulkLoadedCollection::empty() {
        uasserted( 16868, "Cannot empty a collection under-going bulk load." );
    }

    void BulkLoadedCollection::optimizeAll() {
        uasserted( 16895, "Cannot optimize a collection under-going bulk load." );
    }

    void BulkLoadedCollection::optimizePK(const BSONObj &leftPK, const BSONObj &rightPK,
                                          const int timeout, uint64_t *loops_run) {
        uasserted( 16921, "Cannot optimize a collection under-going bulk load." );
    }

    bool BulkLoadedCollection::dropIndexes(const StringData& name, string &errmsg,
                                           BSONObjBuilder &result, bool mayDeleteIdIndex) {
        uasserted( 16894, "Cannot perform drop/dropIndexes on of a collection under-going bulk load." );
    }

    // When closing a BulkLoadedCollection, we need to make sure the key trackers and
    // loaders are destructed before we call up to the parent destructor, because they
    // reference storage::Dictionaries that get destroyed in the parent destructor.
    void BulkLoadedCollection::_close() {
        _loader.reset();
        _multiKeyTrackers.reset();
        NamespaceDetails::close();
    }

    void BulkLoadedCollection::createIndex(const BSONObj &info) {
        uasserted( 16867, "Cannot create an index on a collection under-going bulk load." );
    }

} // namespace mongo
