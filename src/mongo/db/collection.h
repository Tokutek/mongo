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

#pragma once

#include <db.h>

#include "mongo/pch.h"

#include "mongo/base/string_data.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/index.h"
#include "mongo/db/index_set.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/querypattern.h"
#include "mongo/db/storage/builder.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/concurrency/rwlock.h"
#include "mongo/util/concurrency/simplerwlock.h"
#include "mongo/db/queryutil.h"
#include "mongo/s/shardkey.h"

namespace mongo {

    class Collection;
    class CollectionMap;
    class MultiKeyTracker;
    class QueryPattern;

    // Gets a collection - opens it if necessary, but does not create.
    Collection *getCollection(const StringData& ns);

    // Gets the collection map (ns -> Collection) for this client threads' current database.
    // You pass the namespace you want so we can verify you're accessing the right database.
    CollectionMap *collectionMap(const StringData &ns);

    // Used by operations that are supposed to automatically create a collection
    // if it does not exist. Examples include inserts, upsert-style updates, and
    // ensureIndex.
    Collection *getOrCreateCollection(const StringData &ns, const bool logop);

    /** @return true if a client can modify this namespace even though it is under ".system."
        For example <dbname>.system.users is ok for regular clients to update.
        @param write used when .system.js
    */
    bool legalClientSystemNS(const StringData &ns, bool write);

    bool userCreateNS(const StringData &ns, BSONObj options, string &err, bool logForReplication);

    // Add a new entry to the the indexes or namespaces catalog
    void addToIndexesCatalog(const BSONObj &info);
    void addToNamespacesCatalog(const StringData &name, const BSONObj *options = NULL);

    // Used for upgrade 1.4.{0|1} to 1.4.2, due to #1087
    void cleanupOrphanedIndex(const BSONObj& info);

    // Rename a namespace within current 'client' db.
    // (Arguments should include db name)
    void renameCollection(const StringData &from, const StringData &to);
    // Convert a collection within current 'client' db to partitioned.
    // (Arguments should include db name)
    void convertToPartitionedCollection(const StringData& from);

    // Manage bulk loading into a namespace
    //
    // To begin a load, the ns must exist and be empty.
    void beginBulkLoad(const StringData &ns, const vector<BSONObj> &indexes,
                       const BSONObj &options);
    void commitBulkLoad(const StringData &ns);
    void abortBulkLoad(const StringData &ns);

    // Because of #673 we need to detect if we're missing this index and to ignore that error.
    extern BSONObj oldSystemUsersKeyPattern;
    // These are just exposed for tests.
    extern BSONObj extendedSystemUsersKeyPattern;
    extern std::string extendedSystemUsersIndexName;

    bool isSystemUsersCollection(const StringData &ns);

    class CollectionRenamer : boost::noncopyable {
    public:
        virtual ~CollectionRenamer() { }
        virtual void renameCollection(const StringData& from, const StringData& to) = 0;
    };

    //
    // Indexing
    //
    class CollectionIndexer : boost::noncopyable {
    public:
        virtual ~CollectionIndexer() { }
    
        // Prepare an index build. Must be write locked.
        //
        // Must ensure the given Collection will remain valid for
        // the lifetime of the indexer.
        virtual void prepare() = 0;
    
        // Perform the index build. May be read or write locked depending on implementation.
        virtual void build() = 0;
    
        // Commit the index build. Must be write locked.
        //
        // If commit() succeeds (ie: does not throw), the destructor must be called in
        // the same write lock section to prevent a race condition where another thread
        // sets _indexBuildInProgress back to true.
        virtual void commit() = 0;

    };
    
    class CollectionData : boost::noncopyable {
    protected:
        // duplicated (for now)

        // The namespace of this collection, database.collection
        const string _ns;
        // The primary index pattern.
        const BSONObj _pk;

    public:
        virtual ~CollectionData() { }
        virtual bool indexBuildInProgress() const = 0;
        virtual int nIndexes() const = 0;
        const string &ns() const { return _ns; }

        // Close the collection. For regular collections, closes the underlying IndexDetails
        // (and their underlying dictionaries). For bulk loaded collections, closes the
        // loader first and then closes dictionaries. The caller may wish to advise the 
        // implementation that the close() is getting called due to an aborting transaction.
        virtual void close(const bool aborting, bool* indexBitsChanged) = 0;

        // Ensure that the given index exists, or build it if it doesn't.
        // @param info is the index spec (ie: { ns: "test.foo", key: { a: 1 }, name: "a_1", clustering: true })
        // @return whether or the the index was just built.
        virtual bool ensureIndex(const BSONObj &info) = 0;

        /* when a background index build is in progress, we don't count the index in nIndexes until
           complete, yet need to still use it in _indexRecord() - thus we use this function for that.
        */
        virtual int nIndexesBeingBuilt() const = 0;

        virtual IndexDetails& idx(int idxNo) const = 0;

        /* hackish - find our index # in the indexes array */
        virtual int idxNo(const IndexDetails& idx) const = 0;

        // @return offset in indexes[]
        virtual int findIndexByName(const StringData& name) const = 0;

        // @return offset in indexes[]
        virtual int findIndexByKeyPattern(const BSONObj& keyPattern) const = 0;

        // -1 means not found
        virtual int findIdIndex() const = 0;

        virtual bool isPKIndex(const IndexDetails &idx) const = 0;

        virtual IndexDetails &getPKIndex() const = 0;

        // Find by primary key (single element bson object, no field name).
        virtual bool findByPK(const BSONObj &pk, BSONObj &result) const = 0;

        virtual bool isPKHidden() const = 0;

        // Extracts and returns validates an owned BSONObj represetning
        // the primary key portion of the given object. Validates each
        // field, ensuring there are no undefined, regex, or array types.
        virtual BSONObj getValidatedPKFromObject(const BSONObj &obj) const = 0;

        // Extracts and returns an owned BSONObj representing
        // the primary key portion of the given query, if each
        // portion of the primary key exists in the query and
        // is 'simple' (ie: equality, no $ operators)
        virtual BSONObj getSimplePKFromQuery(const BSONObj &query, const BSONObj& pk) const = 0;

        //
        // Write interface
        //
        
        // inserts an object into this namespace, taking care of secondary indexes if they exist
        virtual void insertObject(BSONObj &obj, uint64_t flags, bool* indexBitChanged) = 0;

        // deletes an object from this namespace, taking care of secondary indexes if they exist
        virtual void deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags) = 0;

        // update an object in the namespace by pk, replacing oldObj with newObj
        //
        // handles logging
        virtual void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj,
                                  const bool fromMigrate,
                                  uint64_t flags, bool* indexBitChanged) = 0;

        // currently an unused function
        virtual bool fastupdatesOk() = 0;

        virtual bool updateObjectModsOk() = 0;

        // update an object in the namespace by pk, described by the updateObj's $ operators
        //
        // handles logging
        virtual void updateObjectMods(const BSONObj &pk, const BSONObj &updateObj, 
                                      const bool fromMigrate,
                                      uint64_t flags) = 0;

        // rebuild the given index, online.
        // - if there are options, change those options in the index and update the system catalog.
        // - otherwise, send an optimize message and run hot optimize.
        virtual bool rebuildIndex(int i, const BSONObj &options, BSONObjBuilder &result) = 0;

        virtual void dropIndexDetails(int idxNum, bool noteNs) = 0;
        
        virtual void acquireTableLock() = 0;

        // return true if the namespace is currently under-going bulk load.
        virtual bool bulkLoading() const = 0;

        // optional to implement, return true if the namespace is capped
        virtual bool isCapped() const = 0;

        virtual bool isPartitioned() const = 0;

        // returns true if the CollectionData requires an ID field to exist
        // on insertions. If so, and an id is necessary, then the Collection 
        // class will autogenerate one before calling
        // CollectionData::insertObject
        virtual bool requiresIDField() const = 0;

        // returns the maximum PK the collection's storage knows
        // of. If this CollectionData is part of a partitioned collection,
        // any newly added partition must be greater than or equal to
        // the value returned here
        virtual bool getMaxPKForPartitionCap(BSONObj &result) const = 0;

        // called after dropping indexes
        virtual void finishDrop() = 0;

        virtual void addIndexOK() = 0;

        // struct for storing the accumulated states of a Collection
        // all values, except for nIndexes, are estimates
        // note that the id index is used as the main store.
        struct Stats {
            uint64_t count; // number of rows in id index
            uint64_t size; // size of main store, which is the id index
            uint64_t storageSize; // size on disk of id index
            uint64_t nIndexes; // number of indexes, including id index
            uint64_t indexSize; // size of secondary indexes, NOT including id index
            uint64_t indexStorageSize; // size on disk for secondary indexes, NOT including id index

            Stats() : count(0),
                      size(0),
                      storageSize(0),
                      nIndexes(0),
                      indexSize(0),
                      indexStorageSize(0) {}
            Stats& operator+=(const Stats &o) {
                count += o.count;
                size += o.size;
                storageSize += o.storageSize;
                nIndexes += o.nIndexes;
                indexSize += o.indexSize;
                indexStorageSize += o.indexStorageSize;
                return *this;
            }
            void appendInfo(BSONObjBuilder &b, int scale) const;
        };

        void fillCollectionStats(Stats &aggStats, BSONObjBuilder *result, int scale) const;

        // optional to implement, populate the obj builder with collection specific stats
        virtual void fillSpecificStats(BSONObjBuilder &result, int scale) const = 0;

        virtual shared_ptr<CollectionIndexer> newHotIndexer(const BSONObj &info) = 0;

        virtual unsigned long long getMultiKeyIndexBits() const = 0;

        virtual bool isMultiKey(int i) const = 0;

        // functions that create cursors
        // table scan
        virtual shared_ptr<Cursor> makeCursor(const int direction, const bool countCursor) = 0;

        // index-scan
        virtual shared_ptr<Cursor> makeCursor(const IndexDetails &idx,
                                        const int direction, 
                                        const bool countCursor) = 0;

        // index range scan between start/end
        virtual shared_ptr<Cursor> makeCursor(const IndexDetails &idx,
                                       const BSONObj &startKey, const BSONObj &endKey,
                                       const bool endKeyInclusive,
                                       const int direction, const int numWanted,
                                       const bool countCursor) = 0;

        // index range scan by field bounds
        virtual shared_ptr<Cursor> makeCursor(const IndexDetails &idx,
                                       const shared_ptr<FieldRangeVector> &bounds,
                                       const int singleIntervalLimit,
                                       const int direction, const int numWanted,
                                       const bool countCursor) = 0;

        virtual shared_ptr<CollectionRenamer> getRenamer() = 0;

        template <class T> T *as() {
            T *subclass = dynamic_cast<T *>(this);
            massert(17236, "bug: failed to dynamically cast Collection to desired subclass", subclass != NULL);
            return subclass;
        }
        
        // Key pattern for the primary key. For typical collections, this is { _id: 1 }.
        const BSONObj &pkPattern() const {
            return _pk;
        }


    protected:
        CollectionData(const StringData& ns, const BSONObj &pkIndexPattern) :
            _ns(ns.toString()),
            _pk(pkIndexPattern.copy()) {
        }

        CollectionData(const BSONObj &serialized) :
            _ns(serialized["ns"].String()),
            _pk(serialized["pk"].Obj().copy()) {
        }
        
    };

    // Represents a collection.
    class Collection : boost::noncopyable {
    public:
        static const int NIndexesMax = 64;

        // Flags for write operations. For performance reasons only. Use with caution.
        static const uint64_t NO_LOCKTREE = 1; // skip acquiring locktree row locks
        static const uint64_t NO_UNIQUE_CHECKS = 2; // skip uniqueness checks on all keys
        static const uint64_t KEYS_UNAFFECTED_HINT = 4; // an update did not update secondary indexes
        static const uint64_t NO_PK_UNIQUE_CHECKS = 8; // skip uniqueness checks only on the primary key

        // Creates the appropriate Collection implementation based on options.
        //
        // The bulkLoad parameter is used by beginBulkLoad to open an existing
        // IndexedCollection using a BulkLoadedCollection interface.
        static shared_ptr<Collection> make(const StringData &ns, const BSONObj &options);
        static shared_ptr<Collection> make(const BSONObj &serialized, const bool bulkLoad = false);

        // Find the first object that matches the query. Force index if requireIndex is true.
        static bool findOne(const StringData &ns, const BSONObj &query,
                            BSONObj &result, const bool requireIndex = false);

        virtual ~Collection();

        //
        // Query caching - common to all collections.
        //

        QueryCache &getQueryCache() {
            return _queryCache;
        }

        void notifyOfWriteOp() {
            _queryCache.notifyOfWriteOp();
        }

        //
        // Simple collection metadata - common to all collections.
        //

        // Key pattern for the primary key. For typical collections, this is { _id: 1 }.
        const BSONObj &pkPattern() const {
            return _pk;
        }

        const string &ns() const {
            return _ns;
        }

        const IndexPathSet &indexKeys() const {
            return _indexedPaths;
        }

        // Multikey indexes are indexes where there exists a document with more than
        // one key in the index. Need to dedup queries over these indexes.
        bool isMultikey(int i) const {
            return _cd->isMultiKey(i);
        }

        // 
        // Index access and layout
        //

        void computeIndexKeys();


        /**
         * Record that a new index exists in <dbname>.system.indexes.
         * Only used for the primary key index or an automatic _id index (capped collections),
         * the others go through the normal insert path.
         */
        void addDefaultIndexesToCatalog();

        static BSONObj serialize(const StringData& ns, const BSONObj &options,
                                 const BSONObj &pk, unsigned long long multiKeyIndexBits,
                                 const BSONArray &indexes_array);

        // Serializes metadata to a BSONObj that can be stored on disk for later access.
        // @return a BSON representation of this Collection's state
        BSONObj serialize(const bool includeHotIndex = false) const;

        // Extracts and returns an owned BSONObj representing
        // the primary key portion of the given query, if each
        // portion of the primary key exists in the query and
        // is 'simple' (ie: equality, no $ operators)
        BSONObj getSimplePKFromQuery(const BSONObj &query) const {
            return getSimplePKFromQuery(query, _pk);
        }

        void noteMultiKeyChanged();

        void fillCollectionStats(CollectionData::Stats &aggStats, BSONObjBuilder *result, int scale) const {
            _cd->fillCollectionStats(aggStats, result, scale);
        }

        void noteIndexBuilt();

        //
        // The functions that call into _cd - implementations differ among collection types.
        //
        bool indexBuildInProgress() const {
            return _cd->indexBuildInProgress();
        };
        int nIndexes() const {
            return _cd->nIndexes();
        }

        // Close the collection. For regular collections, closes the underlying IndexDetails
        // (and their underlying dictionaries). For bulk loaded collections, closes the
        // loader first and then closes dictionaries. The caller may wish to advise the 
        // implementation that the close() is getting called due to an aborting transaction.
        void close(const bool aborting = false) {
            bool indexBitsChanged = false;
            _cd->close(aborting, &indexBitsChanged);
            if (indexBitsChanged) {
                noteMultiKeyChanged();
            }
        }

        // Ensure that the given index exists, or build it if it doesn't.
        // @param info is the index spec (ie: { ns: "test.foo", key: { a: 1 }, name: "a_1", clustering: true })
        // @return whether or the the index was just built.
        bool ensureIndex(const BSONObj &info);

        void acquireTableLock() {
            _cd->acquireTableLock();
        }

        shared_ptr<CollectionRenamer> getRenamer() {
            return _cd->getRenamer();
        }

        /* when a background index build is in progress, we don't count the index in nIndexes until
           complete, yet need to still use it in _indexRecord() - thus we use this function for that.
        */
        int nIndexesBeingBuilt() const {
            return _cd->nIndexesBeingBuilt();
        }

        IndexDetails& idx(int idxNo) const {
            return _cd->idx(idxNo);
        }

        /* hackish - find our index # in the indexes array */
        int idxNo(const IndexDetails& idx) const {
            return _cd->idxNo(idx);
        }

        // @return offset in indexes[]
        int findIndexByKeyPattern(const BSONObj& keyPattern) const {
            return _cd->findIndexByKeyPattern(keyPattern);
        }

        /* Returns the index entry for the first index whose prefix contains
         * 'keyPattern'. If 'requireSingleKey' is true, skip indices that contain
         * array attributes. Otherwise, returns NULL.
         */
        const IndexDetails* findIndexByPrefix(const BSONObj &keyPattern ,
                                                      bool requireSingleKey) const;


        // -1 means not found
        int findIdIndex() const {
            return _cd->findIdIndex();
        }

        bool isPKIndex(const IndexDetails &idx) const {
            return _cd->isPKIndex(idx);
        }

        IndexDetails &getPKIndex() const {
            return _cd->getPKIndex();
        }

        // Find by primary key (single element bson object, no field name).
        bool findByPK(const BSONObj &pk, BSONObj &result) const {
            return _cd->findByPK(pk, result);
        }

        bool isPKHidden() const {
            return _cd->isPKHidden();
        }

        // Extracts and returns validates an owned BSONObj represetning
        // the primary key portion of the given object. Validates each
        // field, ensuring there are no undefined, regex, or array types.
        BSONObj getValidatedPKFromObject(const BSONObj &obj) const {
            return _cd->getValidatedPKFromObject(obj);
        }

        // Extracts and returns an owned BSONObj representing
        // the primary key portion of the given query, if each
        // portion of the primary key exists in the query and
        // is 'simple' (ie: equality, no $ operators)
        BSONObj getSimplePKFromQuery(const BSONObj &query, const BSONObj& pk) const {
            return _cd->getSimplePKFromQuery(query, pk);
        }

        //
        // Write interface
        //
        
        // inserts an object into this namespace, taking care of secondary indexes if they exist
        void insertObject(BSONObj &obj, uint64_t flags = 0);

        // deletes an object from this namespace, taking care of secondary indexes if they exist
        void deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags = 0) {
            _cd->deleteObject(pk, obj, flags);
        }

        // update an object in the namespace by pk, replacing oldObj with newObj
        //
        // handles logging
        void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj,
                                  const bool fromMigrate,
                                  uint64_t flags = 0) {
            bool indexBitChanged = false;
            _cd->updateObject(pk, oldObj, newObj, fromMigrate, flags, &indexBitChanged);
            if (indexBitChanged) {
                noteMultiKeyChanged();
            }
        }

        // @return true, if fastupdates are ok for this collection.
        //         fastupdates are not ok for this collection if it's sharded
        //         and the primary key does not contain the full shard key.
        bool fastupdatesOk() {
            return _cd->fastupdatesOk();
        }

        bool updateObjectModsOk() {
            return _cd->updateObjectModsOk();
        }

        // update an object in the namespace by pk, described by the updateObj's $ operators
        //
        // handles logging
        void updateObjectMods(const BSONObj &pk, const BSONObj &updateObj, 
                              const bool fromMigrate,
                              uint64_t flags = 0) {
            _cd->updateObjectMods(pk, updateObj, fromMigrate, flags);
        }

        // Rebuild indexes. Details are implementation specific. This is typically an online operation.
        //
        // @param name, name of the index to optimize. "*" means all indexes
        // @param options, options for the rebuild process. semantics are implementation specific.
        void rebuildIndexes(const StringData &name, const BSONObj &options, BSONObjBuilder &result);

        void drop(string &errmsg, BSONObjBuilder &result, const bool mayDropSystem = false);

        bool dropIndexes(const StringData& name, string &errmsg,
                                 BSONObjBuilder &result, bool mayDeleteIdIndex);

        void dropIndex(const int idxNum);
        
        // return true if the namespace is currently under-going bulk load.
        bool bulkLoading() const {
            return _cd->bulkLoading();
        }

        // optional to implement, return true if the namespace is capped
        bool isCapped() const {
            return _cd->isCapped();
        }

        // optional to implement, return true if the namespace is capped
        bool isPartitioned() const {
            return _cd->isPartitioned();
        }

        // optional to implement, populate the obj builder with collection specific stats
        void fillSpecificStats(BSONObjBuilder &result, int scale) const {
            _cd->fillSpecificStats(result, scale);
        }

        shared_ptr<CollectionIndexer> newHotIndexer(const BSONObj &info) {
            checkAddIndexOK(info);
            // Note this ns in the rollback so if this transaction aborts, we'll
            // close this ns, forcing the next user to reload in-memory metadata.
            CollectionMapRollback &rollback = cc().txn().collectionMapRollback();
            rollback.noteNs(_ns);
            
            return _cd->newHotIndexer(info);
        }

        // Needed for fixing #1087. This should never be called otherwise.
        // Returns true if the index specified in info was not serialized as part of the collection.
        bool indexIsOrphaned(const BSONObj &info) {
            StringData name = info["name"].Stringdata();
            // The name is what determines the dname in the fractal tree, so this is what we check.
            int idxNum = _cd->findIndexByName(name);
            if (idxNum < 0) {
                // We could not find an index with this name, index is orphaned.
                return true;
            }

            // We found an index with this name, looks like it isn't orphaned.  Do a sanity check
            // and make sure the key is what we expect it to be as well.
            // TODO: We might want to also check the rest of info matches.
            massert(17349, mongoutils::str::stream()
                    << "Found an index whose name matches (" << name << ") "
                    << "but its key does not match (expected " << info.getObjectField("key")
                    << ", found " << _cd->idx(idxNum).keyPattern() << ").",
                    _cd->findIndexByKeyPattern(info.getObjectField("key")) == idxNum);

            return false;
        }

        //
        // Subclass detection and type coersion.
        //
        // To keep the Collection interface lean, special functionality is
        // accessed only through a specific child class interface. We know
        // use these booleans to detect when a Collection implementation
        // has special functionality, and we coerce it to a subtype with as().
        //
        // Interpret this Collection as a subclass. Asserts if conversion fails.
        template <class T> T *as() {
            T *subclass = dynamic_cast<T *>(this->_cd.get());
            massert(17230, "bug: failed to dynamically cast Collection to desired subclass", subclass != NULL);
            return subclass;
        }
    protected:
        Collection(const StringData &ns, const BSONObj &options);
        Collection(const BSONObj &serialized, const bool bulkLoad);

        // The namespace of this collection, database.collection
        const string _ns;

        // The options used to create this namespace details. We serialize
        // this (among other things) to disk on close (see serialize())
        BSONObj _options;

        // The primary index pattern.
        // This is non-const because of how
        // the value is filled in the constructor
        BSONObj _pk;

        // Every index has an IndexDetails that describes it.
        IndexPathSet _indexedPaths;
        void resetTransient();
        
        void checkAddIndexOK(const BSONObj &info);

        /* query cache (for query optimizer) */
        QueryCache _queryCache;

        // fix later
        shared_ptr<CollectionData> _cd;
    };

    // Implementation of the collection interface using a simple 
    // std::vector of IndexDetails, the first of which is the primary key.
    class CollectionBase : public CollectionData {
    public:
        virtual ~CollectionBase() { }

        // Close the collection. For regular collections, closes the underlying IndexDetails
        // (and their underlying dictionaries). For bulk loaded collections, closes the
        // loader first and then closes dictionaries. The caller may wish to advise the 
        // implementation that the close() is getting called due to an aborting transaction.
        virtual void close(const bool aborting, bool* indexBitsChanged);

        // Ensure that the given index exists, or build it if it doesn't.
        // @param info is the index spec (ie: { ns: "test.foo", key: { a: 1 }, name: "a_1", clustering: true })
        // @return whether or the the index was just built.
        bool ensureIndex(const BSONObj &info);

        /* when a background index build is in progress, we don't count the index in nIndexes until
           complete, yet need to still use it in _indexRecord() - thus we use this function for that.
        */
        int nIndexesBeingBuilt() const { 
            if (_indexBuildInProgress) {
                verify(_nIndexes + 1 == (int) _indexes.size());
            } else {
                verify(_nIndexes == (int) _indexes.size());
            }
            return _indexes.size();
        }

        IndexDetails& idx(int idxNo) const {
            verify( idxNo < Collection::NIndexesMax );
            verify( idxNo >= 0 && idxNo < (int) _indexes.size() );
            return *_indexes[idxNo];
        }

        /* hackish - find our index # in the indexes array */
        int idxNo(const IndexDetails& idx) const {
            for (IndexVector::const_iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
                const IndexDetails *index = it->get();
                if (index == &idx) {
                    return it - _indexes.begin();
                }
            }
            msgasserted(17229, "E12000 idxNo fails");
            return -1;
        }

        // @return offset in indexes[]
        int findIndexByName(const StringData& name) const;

        // @return offset in indexes[]
        int findIndexByKeyPattern(const BSONObj& keyPattern) const;

        /* @return -1 = not found
           generally id is first index, so not that expensive an operation (assuming present).
        */
        int findIdIndex() const {
            for (IndexVector::const_iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
                const IndexDetails *index = it->get();
                if (index->isIdIndex()) {
                    return it - _indexes.begin();
                }
            }
            return -1;
        }

        bool isPKIndex(const IndexDetails &idx) const {
            const bool isPK = &idx == &getPKIndex();
            dassert(isPK == (idx.keyPattern() == _pk));
            return isPK;
        }

        IndexDetailsBase &getPKIndexBase() const {
            IndexDetailsBase &idx = *_indexes[0];
            dassert(idx.keyPattern() == _pk);
            return idx;
        }

        IndexDetails &getPKIndex() const {
            return getPKIndexBase();
        }

        bool indexBuildInProgress() const {
            return _indexBuildInProgress;
        }

        int nIndexes() const {
            return _nIndexes;
        }

        // Find by primary key (single element bson object, no field name).
        bool findByPK(const BSONObj &pk, BSONObj &result) const;

        // @return true, if fastupdates are ok for this collection.
        //         fastupdates are not ok for this collection if it's sharded
        //         and the primary key does not contain the full shard key.
        bool fastupdatesOk();
        bool updateObjectModsOk() {
            return true;
        }

        // Extracts and returns validates an owned BSONObj represetning
        // the primary key portion of the given object. Validates each
        // field, ensuring there are no undefined, regex, or array types.
        virtual BSONObj getValidatedPKFromObject(const BSONObj &obj) const;

        // return true if the namespace is currently under-going bulk load.
        virtual bool bulkLoading() const {
            return false;
        }

        // optional to implement, return true if the namespace is capped
        virtual bool isCapped() const {
            return false;
        }

        virtual bool isPartitioned() const {
            return false;
        }

        virtual bool getMaxPKForPartitionCap(BSONObj &result) const;

        virtual void addIndexOK() { }

        virtual void finishDrop() { }

        // Extracts and returns an owned BSONObj representing
        // the primary key portion of the given query, if each
        // portion of the primary key exists in the query and
        // is 'simple' (ie: equality, no $ operators)
        virtual BSONObj getSimplePKFromQuery(const BSONObj &query, const BSONObj& pk) const;

        // inserts an object into this namespace, taking care of secondary indexes if they exist
        virtual void insertObject(BSONObj &obj, uint64_t flags, bool* indexBitChanged) = 0;

        // deletes an object from this namespace, taking care of secondary indexes if they exist
        virtual void deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags);

        // update an object in the namespace by pk, replacing oldObj with newObj
        //
        // handles logging
        virtual void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj,
                                  const bool fromMigrate,
                                  uint64_t flags, bool* indexBitChanged);

        // update an object in the namespace by pk, described by the updateObj's $ operators
        //
        // handles logging
        virtual void updateObjectMods(const BSONObj &pk, const BSONObj &updateObj, 
                                      const bool fromMigrate,
                                      uint64_t flags);
        
        void setIndexIsMultikey(const int idxNum, bool* indexBitChanged);

        class IndexerBase : public CollectionIndexer {
        public:
            // Prepare an index build. Must be write locked.
            //
            // Must ensure the given Collection will remain valid for
            // the lifetime of the indexer.
            void prepare();

            // Perform the index build. May be read or write locked depending on implementation.
            virtual void build() = 0;

            // Commit the index build. Must be write locked.
            //
            // If commit() succeeds (ie: does not throw), the destructor must be called in
            // the same write lock section to prevent a race condition where another thread
            // sets _indexBuildInProgress back to true.
            void commit();

        protected:
            IndexerBase(CollectionBase *cl, const BSONObj &info);
            // Must be write locked for destructor.
            virtual ~IndexerBase();

            // Indexer implementation specifics.
            virtual void _prepare() { }
            virtual void _commit() { }

            CollectionBase *_cl;
            shared_ptr<IndexDetailsBase> _idx;
            const BSONObj &_info;
            const bool _isSecondaryIndex;
        };

        // Indexer for background (aka hot, aka online) indexing.
        // build() should be called read locked, not write locked.
        class HotIndexer : public IndexerBase {
        public:
            HotIndexer(CollectionBase *cl, const BSONObj &info);
            virtual ~HotIndexer() { }

            void build();

        private:
            void _prepare();
            void _commit();
            scoped_ptr<MultiKeyTracker> _multiKeyTracker;
            scoped_ptr<storage::Indexer> _indexer;
        };

        // Indexer for foreground (aka cold, aka offline) indexing.
        // build() must be called write locked.
        //
        // Cold indexing is theoretically faster than hot indexing at
        // the expense of holding the write lock for a long time.
        class ColdIndexer : public IndexerBase {
        public:
            ColdIndexer(CollectionBase *cl, const BSONObj &info);
            virtual ~ColdIndexer() { }

            void build();
        };

        shared_ptr<CollectionIndexer> newIndexer(const BSONObj &info, const bool background);
        virtual shared_ptr<CollectionIndexer> newHotIndexer(const BSONObj &info);

        // optional to implement, populate the obj builder with collection specific stats
        virtual void fillSpecificStats(BSONObjBuilder &result, int scale) const {
        }

        unsigned long long getMultiKeyIndexBits() const {
            return _multiKeyIndexBits;
        }

        // rebuild the given index, online.
        // - if there are options, change those options in the index and update the system catalog.
        // - otherwise, send an optimize message and run hot optimize.
        bool rebuildIndex(int i, const BSONObj &options, BSONObjBuilder &result);

        virtual void dropIndexDetails(int idxNum, bool noteNs);

        void acquireTableLock();

        bool isMultiKey(int i) const {
            const unsigned long long mask = 1ULL << i;
            return (_multiKeyIndexBits & mask) != 0;
        }

        // table scan
        virtual shared_ptr<Cursor> makeCursor(const int direction, const bool countCursor);

        // index-scan
        virtual shared_ptr<Cursor> makeCursor(const IndexDetails &idx,
                                        const int direction, 
                                        const bool countCursor);

        // index range scan between start/end
        virtual shared_ptr<Cursor> makeCursor(const IndexDetails &idx,
                                       const BSONObj &startKey, const BSONObj &endKey,
                                       const bool endKeyInclusive,
                                       const int direction, const int numWanted,
                                       const bool countCursor);

        // index range scan by field bounds
        virtual shared_ptr<Cursor> makeCursor(const IndexDetails &idx,
                                       const shared_ptr<FieldRangeVector> &bounds,
                                       const int singleIntervalLimit,
                                       const int direction, const int numWanted,
                                       const bool countCursor);

        class Renamer : public CollectionRenamer {
            vector<BSONObj> _indexSpecs;
        public:
            Renamer(CollectionBase* cl) {
                for (int i = 0; i < cl->_nIndexes; i++) {
                    _indexSpecs.push_back(cl->idx(i).info().copy());
                }
            }

            // not sure if it is necessary to pass in from, whether this is the
            // same as _ns
            virtual void renameCollection(const StringData& from, const StringData& to) {
                for ( vector<BSONObj>::const_iterator it = _indexSpecs.begin() ; it != _indexSpecs.end(); it++) {
                    BSONObj oldIndexSpec = *it;
                    string idxName = oldIndexSpec["name"].String();
                    string oldIdxNS = IndexDetails::indexNamespace(from, idxName);
                    string newIdxNS = IndexDetails::indexNamespace(to, idxName);
                    TOKULOG(1) << "renaming " << oldIdxNS << " to " << newIdxNS << endl;
                    storage::db_rename(oldIdxNS, newIdxNS);
                }
            }
        };

        virtual shared_ptr<CollectionRenamer> getRenamer() {
            shared_ptr<CollectionRenamer> ret;
            ret.reset(new Renamer(this));
            return ret;
        }

    protected:
        CollectionBase(const StringData& ns, const BSONObj &pkIndexPattern, const BSONObj &options);
        explicit CollectionBase(const BSONObj &serialized, bool* reserializeNeeded = NULL);

        virtual void createIndex(const BSONObj &info);
        void checkIndexUniqueness(const IndexDetailsBase &idx);

        void insertIntoIndexes(const BSONObj &pk, const BSONObj &obj, uint64_t flags, bool* indexBitChanged);
        void deleteFromIndexes(const BSONObj &pk, const BSONObj &obj, uint64_t flags);

        // uassert on duplicate key
        void checkUniqueIndexes(const BSONObj &pk, const BSONObj &obj);

        typedef std::vector<shared_ptr<IndexDetailsBase> > IndexVector;
        IndexVector _indexes;

        bool _indexBuildInProgress;
        int _nIndexes;

        unsigned long long _multiKeyIndexBits;

        static bool _allowSetMultiKeyInMSTForTests;
    public:
        // INTERNAL ONLY
        // The dbtests like to use long-lived transactions to get out of having to specify transactions for each little thing.
        // Some dbtests need to set the multikey bit.
        // This flag, if on, skips the check that prevents users from setting the multiKey bit in an MST, which lets some dbtests pass.
        // This should NEVER be true in production.
        static void turnOnAllowSetMultiKeyInMSTForTests() {
            _allowSetMultiKeyInMSTForTests = true;
        }

    private:
        struct findByPKCallbackExtra : public ExceptionSaver {
            BSONObj &obj;
            findByPKCallbackExtra(BSONObj &o) : obj(o) { }
        };
        static int findByPKCallback(const DBT *key, const DBT *value, void *extra);
        static int getLastKeyCallback(const DBT *key, const DBT *value, void *extra);

        // @return the smallest (in terms of dataSize, which is key length + value length)
        //         index in _indexes that is one-to-one with the primary key. specifically,
        //         the returned index cannot be sparse or multikey.
        IndexDetails &findSmallestOneToOneIndex() const;
    };

    class IndexedCollection : public CollectionBase {
    private:
        const bool _idPrimaryKey;

    public:
        IndexedCollection(const StringData &ns, const BSONObj &options);
        IndexedCollection(const BSONObj &serialized, bool* reserializeNeeded = NULL);

        // inserts an object into this namespace, taking care of secondary indexes if they exist
        void insertObject(BSONObj &obj, uint64_t flags, bool* indexBitChanged);

        void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj,
                          const bool fromMigrate,
                          uint64_t flags, bool* indexBitChanged);
        
        bool isPKHidden() const {
            return false;
        }

        bool requiresIDField() const {
            return true;
        }

        // Overridden to optimize the case where we have an _id primary key.
        BSONObj getValidatedPKFromObject(const BSONObj &obj) const;

        // Overriden to optimize pk generation for an _id primary key.
        // We just need to look for the _id field and, if it exists
        // and is simple, return a wrapped object.
        BSONObj getSimplePKFromQuery(const BSONObj &query, const BSONObj& pk) const;
    };

    // Virtual interface implemented by collections whose cursors may "tail"
    // the end of the collection for newly arriving data.
    //
    // Only the oplog and capped collections suppot this feature.
    class TailableCollection {
    public:
        // @return the minimum key that is not safe to read for any tailable cursor
        virtual BSONObj minUnsafeKey() = 0;
        virtual ~TailableCollection() { }
    };

    class NaturalOrderCollection : public CollectionBase {
    public:
        NaturalOrderCollection(const StringData &ns, const BSONObj &options);
        NaturalOrderCollection(const BSONObj &serialized);

        // insert an object, using a fresh auto-increment primary key
        void insertObject(BSONObj &obj, uint64_t flags, bool* indexBitChanged);

        bool isPKHidden() const {
            return true;
        }
        
        bool requiresIDField() const {
            return false;
        }

    protected:
        AtomicWord<long long> _nextPK;
    };

    class SystemCatalogCollection : public NaturalOrderCollection {
    public:
        SystemCatalogCollection(const StringData &ns, const BSONObj &options);
        SystemCatalogCollection(const BSONObj &serialized);

        // strip out the _id field before inserting into a system collection
        void insertObject(BSONObj &obj, uint64_t flags, bool* indexBitChanged);

    private:
        void createIndex(const BSONObj &info);

        // For consistency with Vanilla MongoDB, the system catalogs have the following
        // fields, in order, if they exist.
        //
        //  { key, unique, ns, name, [everything else] }
        //
        // This code is largely borrowed from prepareToBuildIndex() in Vanilla.
        BSONObj beautify(const BSONObj &obj);
    };

    // Class representing the system catalogs.
    // Used for:
    // - db.system.indexes
    // - db.system.namespaces
    class SystemUsersCollection : public IndexedCollection {
    public:
        static BSONObj extendedSystemUsersIndexInfo(const StringData &ns);
        SystemUsersCollection(const StringData &ns, const BSONObj &options);
        SystemUsersCollection(const BSONObj &serialized, bool* reserializeNeeded);
        void insertObject(BSONObj &obj, uint64_t flags, bool* indexBitChanged);
        void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj,
                          const bool fromMigrate,
                          uint64_t flags, bool* indexBitChanged);
        void updateObjectMods(const BSONObj &pk, const BSONObj &updateobj,
                              const bool fromMigrate,
                              uint64_t flags);
        bool updateObjectModsOk() {
            return false;
        }
    };

    // Capped collections have natural order insert semantics but borrow (ie: copy)
    // its document modification strategy from IndexedCollections. The size
    // and count of a capped collection is maintained in memory and kept valid
    // on txn abort through a CappedCollectionRollback class in the TxnContext. 
    //
    // TailableCollection cursors over capped collections may only read up to one less
    // than the minimum uncommitted primary key to ensure that they never miss
    // any data. This information is communicated through minUnsafeKey(). On
    // commit/abort, the any primary keys inserted into a capped collection are
    // noted so we can properly maintain the min uncommitted key.
    //
    // In the implementation, NaturalOrderCollection::_nextPK and the set of
    // uncommitted primary keys are protected together by _mutex. Trimming
    // work is done under the _deleteMutex.
    class CappedCollection : public NaturalOrderCollection, public TailableCollection {
    public:
        CappedCollection(const StringData &ns, const BSONObj &options,
                         const bool mayIndexId = true);
        CappedCollection(const BSONObj &serialized);

        void fillSpecificStats(BSONObjBuilder &result, int scale) const;

        bool isCapped() const { return true; }

        // @return the minimum key that is not safe to read for any tailable cursor
        BSONObj minUnsafeKey();

        // Regular interface
        void insertObject(BSONObj &obj, uint64_t flags, bool* indexBitChanged);

        void deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags);

        void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj,
                          const bool fromMigrate,
                          uint64_t flags, bool* indexBitChanged);

        void updateObjectMods(const BSONObj &pk, const BSONObj &updateobj,
                              const bool fromMigrate,
                              uint64_t flags);

        bool updateObjectModsOk() {
            return false;
        }

        // Hacked interface for handling oplogging and replaying ops from a secondary.
        void insertObjectAndLogOps(const BSONObj &obj, uint64_t flags, bool* indexBitChanged);

        void insertObjectWithPK(const BSONObj &pk, const BSONObj &obj, uint64_t flags, bool* indexBitChanged);

        void deleteObjectWithPK(const BSONObj &pk, const BSONObj &obj, uint64_t flags);

        // Remove everything from this capped collection
        void empty();

        // Note the commit of a transaction, which simple notes completion under the lock.
        // We don't need to do anything with nDelta and sizeDelta because those changes
        // are already applied to in-memory stats, and this transaction has committed.
        void noteCommit(const BSONObj &minPK, long long nDelta, long long sizeDelta);

        // Note the abort of a transaction, noting completion and updating in-memory stats.
        //
        // The given deltas are signed values that represent changes to the collection.
        // We need to roll back those changes. Therefore, we subtract from the current value.
        void noteAbort(const BSONObj &minPK, long long nDelta, long long sizeDelta);
        
        bool requiresIDField() const {
            return true;
        }

    protected:
        void _insertObject(const BSONObj &obj, uint64_t flags, bool* indexBitChanged);

    private:
        // requires: _mutex is held
        void noteUncommittedPK(const BSONObj &pk);

        BSONObj getNextPK();

        // Note the completion of a transaction by removing its
        // minimum-PK-inserted (if there is one) from the set.
        void noteComplete(const BSONObj &minPK);

        void checkGorged(const BSONObj &obj, bool logop);

        void checkUniqueIndexes(const BSONObj &pk, const BSONObj &obj, bool checkPk);

        // Checks unique indexes and does the actual inserts.
        // Does not check if the collection became gorged.
        void checkUniqueAndInsert(const BSONObj &pk, const BSONObj &obj, uint64_t flags, bool checkPk, bool* indexBitChanged);

        bool isGorged(long long n, long long size) const;

        void _deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags);

        void trim(int objsize, bool logop);

        const long long _maxSize;
        const long long _maxObjects;
        AtomicWord<long long> _currentObjects;
        AtomicWord<long long> _currentSize;
        BSONObj _lastDeletedPK;
        // The set of minimum-uncommitted-PKs for this capped collection.
        // Each transaction that has done inserts has the minimum PK it
        // inserted in this set.
        //
        // TailableCollection cursors must not read at or past the smallest value in this set.
        BSONObjSet _uncommittedMinPKs;
        SimpleMutex _mutex;
        SimpleMutex _deleteMutex;
    };

    // Profile collections are non-replicated capped collections that
    // cannot be updated and do not add the _id field on insert.
    class ProfileCollection : public CappedCollection {
    public:
        ProfileCollection(const StringData &ns, const BSONObj &options);
        ProfileCollection(const BSONObj &serialized);

        void insertObject(BSONObj &obj, uint64_t flags, bool* indexBitChanged);

        void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj,
                          const bool fromMigrate,
                          uint64_t flags, bool* indexBitChanged);

        void updateObjectMods(const BSONObj &pk, const BSONObj &updateobj,
                              const bool fromMigrate,
                              uint64_t flags);

    private:
        void createIndex(const BSONObj &idx_info);
    };

    // A BulkLoadedCollection is a facade for an IndexedCollection that utilizes
    // a bulk loader for insertions. Other flavors of writes are not allowed.
    //
    // The underlying indexes must exist and be empty.
    class BulkLoadedCollection : public IndexedCollection {
    public:
        BulkLoadedCollection(const BSONObj &serialized);

        bool bulkLoading() const { return true; }

        void close(const bool abortingLoad, bool* indexBitsChanged);

        void validateConnectionId(const ConnectionId &id);

        void insertObject(BSONObj &obj, uint64_t flags, bool* indexBitChanged);

        void deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags);

        void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj,
                          const bool fromMigrate,
                          uint64_t flags, bool* indexBitChanged);

        void updateObjectMods(const BSONObj &pk, const BSONObj &updateobj,
                              const bool fromMigrate,
                              uint64_t flags);

        void empty();

        bool rebuildIndex(int i, const BSONObj &options, BSONObjBuilder &wasBuilder);

        void dropIndexDetails(int idxNum, bool noteNs);

        void addIndexOK();

    private:
        // When closing a BulkLoadedCollection, we need to make sure the key trackers and
        // loaders are destructed before we call up to the parent destructor, because they
        // reference storage::Dictionaries that get destroyed in the parent destructor.
        void _close(bool aborting, bool* indexBitsChanged);

        void createIndex(const BSONObj &info);

        // The connection that started the bulk load is the only one that can
        // do anything with the namespace until the load is complete and this
        // namespace has been closed / re-opened.
        ConnectionId _bulkLoadConnectionId;
        scoped_array<DB *> _dbs;
        scoped_array< scoped_ptr<MultiKeyTracker> > _multiKeyTrackers;
        scoped_ptr<storage::Loader> _loader;
    };

    string getMetaCollectionName(const StringData &ns);
    string getPartitionName(const StringData &ns, uint64_t partitionID);

    class PartitionedCollection : public CollectionData {
    public:
        //
        // The CollectionData interface
        // See CollectionData to understand what functions are
        // supposed to do
        //
        
        // partitioned collections cannot build hot indexes
        virtual bool indexBuildInProgress() const {
            return false;
        }
        virtual int nIndexes() const {
            return _partitions[0]->nIndexes();
        }

        virtual void close(const bool aborting, bool* indexBitsChanged) {
            for (IndexCollVector::const_iterator it = _partitions.begin(); 
                 it != _partitions.end(); 
                 ++it) 
            {
                CollectionData *currColl = it->get();
                currColl->close(aborting, indexBitsChanged);
            }
            _metaCollection->close(aborting, indexBitsChanged);
        }

        virtual bool ensureIndex(const BSONObj &info);

        virtual int nIndexesBeingBuilt() const {
            return _partitions[0]->nIndexesBeingBuilt();
        }

        virtual IndexDetails& idx(int idxNo) const {
            return *_indexDetailsVector[idxNo];
        }

        virtual int idxNo(const IndexDetails& idx) const {
            for (PartitionedIndexVector::const_iterator it = _indexDetailsVector.begin(); 
                 it != _indexDetailsVector.end();
                 ++it)
            {
                const IndexDetails *index = it->get();
                if (index == &idx) {
                    return it - _indexDetailsVector.begin();
                }
            }
            msgasserted(17244, "E12000 idxNo fails");
            return -1;
        }

        virtual int findIndexByName(const StringData& name) const {
            return _partitions[0]->findIndexByName(name);
        }

        virtual int findIndexByKeyPattern(const BSONObj& keyPattern) const {
            return _partitions[0]->findIndexByKeyPattern(keyPattern);
        }

        virtual int findIdIndex() const {
            return _partitions[0]->findIdIndex();
        }

        virtual bool isPKIndex(const IndexDetails &idx) const {
            const bool isPK = &idx == &getPKIndex();
            dassert(isPK == (idx.keyPattern() == _pk));
            return isPK;
        }
        virtual IndexDetails &getPKIndex() const {
            IndexDetails &idx = *_indexDetailsVector[0];
            dassert(idx.keyPattern() == _pk);
            return idx;
        }

        virtual bool findByPK(const BSONObj &pk, BSONObj &result) const {
            uint64_t whichPartition = partitionWithPK(pk);
            return _partitions[whichPartition]->findByPK(pk, result);
        }

        virtual bool isPKHidden() const {
            return _partitions[0]->isPKHidden();
        }
        
        virtual BSONObj getValidatedPKFromObject(const BSONObj &obj) const {
            // it does not matter which partition we answer this from
            // it should all be the same
            return _partitions[0]->getValidatedPKFromObject(obj);
        }

        virtual BSONObj getSimplePKFromQuery(const BSONObj &query, const BSONObj& pk) const {
            // it does not matter which partition we answer this from
            // it should all be the same
            return _partitions[0]->getSimplePKFromQuery(query, pk);
        }
        
        virtual void insertObject(BSONObj &obj, uint64_t flags, bool* indexBitChanged) {
            uint64_t whichPartition = partitionWithRow(obj);
            _partitions[whichPartition]->insertObject(obj, flags, indexBitChanged);
        }

        virtual void deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
            uint64_t whichPartition = partitionWithPK(pk);
            _partitions[whichPartition]->deleteObject(pk, obj, flags);
        }

        virtual void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj,
                                  const bool fromMigrate,
                                  uint64_t flags, bool* indexBitChanged);

        virtual bool fastupdatesOk() {
            return _partitions[0]->fastupdatesOk();
        }

        virtual bool updateObjectModsOk() {
            return true;
        }

        virtual void updateObjectMods(const BSONObj &pk, const BSONObj &updateObj, 
                                      const bool fromMigrate,
                                      uint64_t flags) {
            uint64_t whichPartition = partitionWithPK(pk);
            _partitions[whichPartition]->updateObjectMods(pk, updateObj, fromMigrate, flags);
        }

        virtual bool rebuildIndex(int i, const BSONObj &options, BSONObjBuilder &result);

        virtual void dropIndexDetails(int idxNum, bool noteNs) {
            // get rid of the index details            
            if (noteNs) {
                // Note this ns in the rollback so if this transaction aborts, we'll
                // close this ns, forcing the next user to reload in-memory metadata.
                CollectionMapRollback &rollback = cc().txn().collectionMapRollback();
                rollback.noteNs(_ns);
            }
            _indexDetailsVector.erase(_indexDetailsVector.begin() + idxNum);
            for (IndexCollVector::const_iterator it = _partitions.begin(); 
                 it != _partitions.end(); 
                 ++it) 
            {
                CollectionData *currColl = it->get();
                currColl->dropIndexDetails(idxNum, false);
            }
        }
        
        virtual void acquireTableLock() {
            uasserted(17241, "Cannot get a table lock on a partitioned collection");
        }

        virtual bool bulkLoading() const {
            return false;
        }

        virtual bool isCapped() const {
            return false;
        }

        virtual bool isPartitioned() const {
            return true;
        }

        virtual bool requiresIDField() const {
            return _partitions[0]->requiresIDField();
        }

        virtual bool getMaxPKForPartitionCap(BSONObj &result) const {
            msgasserted(17311, "should not call getMaxPKForPartitionCap on a partitioned collection");
        }

        virtual void fillSpecificStats(BSONObjBuilder &result, int scale) const;

        virtual shared_ptr<CollectionIndexer> newHotIndexer(const BSONObj &info) {
            uasserted(17242, "Cannot create a hot index on a partitioned collection");
        }

        virtual unsigned long long getMultiKeyIndexBits() const;

        // for now, no multikey indexes on partitioned collections
        virtual bool isMultiKey(int i) const;
        
        // table scan
        virtual shared_ptr<Cursor> makeCursor(const int direction, const bool countCursor);

        // index-scan
        virtual shared_ptr<Cursor> makeCursor(const IndexDetails &idx,
                                        const int direction, 
                                        const bool countCursor);

        // index range scan between start/end
        virtual shared_ptr<Cursor> makeCursor(const IndexDetails &idx,
                                       const BSONObj &startKey, const BSONObj &endKey,
                                       const bool endKeyInclusive,
                                       const int direction, const int numWanted,
                                       const bool countCursor);

        // index range scan by field bounds
        virtual shared_ptr<Cursor> makeCursor(const IndexDetails &idx,
                                       const shared_ptr<FieldRangeVector> &bounds,
                                       const int singleIntervalLimit,
                                       const int direction, const int numWanted,
                                       const bool countCursor);


        class Renamer : public CollectionRenamer {
            shared_ptr<CollectionRenamer> _metaRenamer;
            std::vector< shared_ptr<CollectionRenamer> > _partitionRenamers;
            std::vector<uint64_t> _ids;
        public:
            Renamer(PartitionedCollection *pc) {
                _metaRenamer = pc->_metaCollection->getRenamer();
                for (uint64_t i = 0; i < pc->numPartitions(); i++) {
                    _partitionRenamers.push_back(pc->_partitions[i]->getRenamer());
                    _ids.push_back(pc->_partitionIDs[i]);
                }
            }

            // not sure if it is necessary to pass in from, whether this is the
            // same as _ns
            virtual void renameCollection(const StringData& from, const StringData& to) {
                _metaRenamer->renameCollection(
                    getMetaCollectionName(from),
                    getMetaCollectionName(to)
                    );
                uint64_t curr = 0;
                for ( vector< shared_ptr<CollectionRenamer> >::const_iterator it = _partitionRenamers.begin(); 
                      it != _partitionRenamers.end(); 
                      it++
                      )
                {
                    (*it)->renameCollection(
                        getPartitionName(from, _ids[curr]),
                        getPartitionName(to, _ids[curr])
                        );
                    curr++;
                }
            }
        };
        shared_ptr<CollectionRenamer> getRenamer() {
            shared_ptr<CollectionRenamer> ret(new Renamer (this));
            return ret;
        }

        // functions for adding/dropping partitions
        void dropPartition(uint64_t id);
        void addPartition();
        void manuallyAddPartition(const BSONObj& newPivot);
        void addPartitionFromOplog(const BSONObj& newPivot, const BSONObj &partitionInfo);
        void getPartitionInfo(uint64_t* numPartitions, BSONArray* partitionArray);
        void addClonedPartitionInfo(const vector<BSONElement> &partitionInfo);
        BSONObj getPartitionMetadata(uint64_t index);
        void updatePartitionMetadata(uint64_t index, BSONObj newMetadata, bool checkCreateTime = true);

        // helper functions
        uint64_t numPartitions() const{
            return _partitions.size();;
        }

        // return the partition at offset index, note this is NOT the partition ID
        shared_ptr<CollectionData> getPartition(uint64_t idx) {
            massert(17254, mongoutils::str::stream() << "invalid index " << idx << " for partition (max: " << numPartitions() << ")",
                    idx < numPartitions());
            return _partitions[idx];
        }
        // states which partition the row or PK belongs to
        uint64_t partitionWithPK(const BSONObj& pk) const;
        uint64_t partitionWithRow(const BSONObj& row) const {
            return partitionWithPK(getValidatedPKFromObject(row));
        }
        shared_ptr<CollectionData> getMetaCollection() {
            return _metaCollection;
        }

        static shared_ptr<PartitionedCollection> make(const StringData &ns, const BSONObj &options);
        static shared_ptr<PartitionedCollection> make(const BSONObj &serialized, CollectionRenamer* renamer);
        static shared_ptr<PartitionedCollection> make(const BSONObj &serialized);

        virtual void finishDrop();
        void addIndexOK() { }

    protected:
        // make constructors
        // called in appendNewPartition. This is the method (that can be overridden),
        // that creates a new partition
        virtual shared_ptr<CollectionData> makeNewPartition(const StringData &ns, const BSONObj &options);
        // called in constructor
        virtual shared_ptr<CollectionData> openExistingPartition(const BSONObj &serialized);
        PartitionedCollection(const StringData &ns, const BSONObj &options);
        PartitionedCollection(const BSONObj &serialized, CollectionRenamer* renamer);
        PartitionedCollection(const BSONObj &serialized);
        void initialize(const StringData &ns, const BSONObj &options);
        void initialize(const BSONObj &serialized, CollectionRenamer* renamer);
        void initialize(const BSONObj &serialized);        
    private:
        void createIndexDetails();
        void sanityCheck();

        // function used internally to drop a partition
        void dropPartitionInternal(uint64_t id);
        // helper function that runs common code for the various
        // methods that add a partition
        void prepareAddPartition();
        // helper function, adds a partition as specified by partitionInfo
        // used by appendNewPartition and createPartitionsFromClone
        void appendPartition(BSONObj partitionInfo);
        // adds a partition, does NOT cap previous partition
        // So, from a user's perspective, this function only does a 
        // subset of what is needed to add a partition to the system
        void appendNewPartition();
        // for pivot associated with ith partition (note, not the id),
        // replace the pivot with newPivot, both in _metaCollection
        // and in _partitionPivots
        void overwritePivot(uint64_t i, const BSONObj& newPivot);
        // these next two functions cap the last partition with a pivot
        // that is something other than MaxKey
        void capLastPartition();
        void manuallyCapLastPartition(const BSONObj& newPivot);
        // finds the index into our vectors that has stuff (collection, pivot, etc...)
        // according to this id
        uint64_t findInMemoryPartition(uint64_t id);

        // return upper bound
        BSONObj getUpperBound();


        // options to be used when creating new partitions
        BSONObj _options;

        typedef std::vector<shared_ptr<CollectionData> > IndexCollVector;
        // the partitions
        IndexCollVector _partitions;
        // Collection storing metadata about the PartitionedCollection
        shared_ptr<IndexedCollection> _metaCollection;

        typedef std::vector<shared_ptr<PartitionedIndexDetails> > PartitionedIndexVector;
        PartitionedIndexVector _indexDetailsVector; // one per index, which at this time means only one total

        // vector storing the ids of the partitions
        // This information is also stored in _metaCollection, but is cached
        // here for convenience.
        std::vector<uint64_t> _partitionIDs;

        // The value of the ith element is the 
        // maximum possible key that may be stored in the ith
        // partition (with INFINITY being the implicit limit on the last
        // partition. So, if the (i-1)th value is 100 and the ith value is 200,
        // then partition i stores values x such that 100 < x <= 200
        std::vector<BSONObj> _partitionPivots;
        Ordering _ordering; // used for comparisons

        // for makeCursor, to determine what partitions we needto visit
        const ShardKeyPattern _shardKeyPattern;
    };

    // for legacy oplogs that were not partitioned. So we can open them just long enough
    // to convert them to partitioned
    class OldOplogCollection : public IndexedCollection, public TailableCollection {
    public:
        OldOplogCollection(const StringData &ns, const BSONObj &options);
        // Important: BulkLoadedCollection relies on this constructor
        // doing nothing more than calling the parent IndexedCollection
        // constructor. If this constructor ever does more, we need to
        // modify BulkLoadedCollection to match behavior for the oplog.
        OldOplogCollection(const BSONObj &serialized);

        // @return the minimum key that is not safe to read for any tailable cursor
        BSONObj minUnsafeKey();
    };

    // what the individual partitions of the oplog are
    // the reason we create this class is so we can also be tailable
    class OplogPartition : public IndexedCollection, public TailableCollection {
    public:
        OplogPartition(const StringData &ns, const BSONObj &options);
        // Important: BulkLoadedCollection relies on this constructor
        // doing nothing more than calling the parent IndexedCollection
        // constructor. If this constructor ever does more, we need to
        // modify BulkLoadedCollection to match behavior for the oplog.
        OplogPartition(const BSONObj &serialized);
    
        // @return the minimum key that is not safe to read for any tailable cursor
        BSONObj minUnsafeKey();
    };

    // The real oplog, OldOplogCollection is just temporary
    class PartitionedOplogCollection : public PartitionedCollection {
    public:
        static shared_ptr<PartitionedOplogCollection> make(const StringData &ns, const BSONObj &options);
        static shared_ptr<PartitionedOplogCollection> make(const BSONObj &serialized);
    protected:
        PartitionedOplogCollection(const StringData &ns, const BSONObj &options);
        // Important: BulkLoadedCollection relies on this constructor
        // doing nothing more than calling the parent IndexedCollection
        // constructor. If this constructor ever does more, we need to
        // modify BulkLoadedCollection to match behavior for the oplog.
        PartitionedOplogCollection(const BSONObj &serialized);
        // called in appendNewPartition. This is the method (that can be overridden),
        // that creates a new partition
        virtual shared_ptr<CollectionData> makeNewPartition(const StringData &ns, const BSONObj &options);
        // called in constructor
        virtual shared_ptr<CollectionData> openExistingPartition(const BSONObj &serialized);
    };


} // namespace mongo
