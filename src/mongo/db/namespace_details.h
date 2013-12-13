// namespace_details.h

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

#pragma once

#include <db.h>

#include "mongo/pch.h"

#include "mongo/db/namespacestring.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/index.h"
#include "mongo/db/index_set.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/queryoptimizercursor.h"
#include "mongo/db/query_plan_selection_policy.h"
#include "mongo/db/querypattern.h"
#include "mongo/db/relock.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/dictionary.h"
#include "mongo/db/storage/builder.h"
#include "mongo/util/concurrency/simplerwlock.h"
#include "mongo/util/string_map.h"

namespace mongo {

    class CollectionMap;

    /** @return true if a client can modify this namespace even though it is under ".system."
        For example <dbname>.system.users is ok for regular clients to update.
        @param write used when .system.js
    */
    bool legalClientSystemNS( const StringData& ns , bool write );

    bool userCreateNS(const StringData& ns, BSONObj options, string& err, bool logForReplication);

    // used for operations that are supposed to create the namespace if it does not exist,
    // such as insert, some updates, and create index
    NamespaceDetails* getAndMaybeCreateNS(const StringData& ns, bool logop);

    // Add a new entry to the the indexes or namespaces catalog
    void addToIndexesCatalog(const BSONObj &info);
    void addToNamespacesCatalog(const StringData& name, const BSONObj *options = NULL);

    // Rename a namespace within current 'client' db.
    // (Arguments should include db name)
    void renameNamespace( const StringData& from, const StringData& to );

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

    bool isSystemCatalog(const StringData &ns);
    bool isProfileCollection(const StringData &ns);
    bool isOplogCollection(const StringData &ns);
    bool isSystemUsersCollection(const StringData &ns);

    /* NamespaceDetails : this is the information about a namespace.
       It is stored in a serialized form (see serialize()) in the CollectionMap, stored
       on disk as a ydb dictionary named foo.ns, for Database foo).
    */

    class NamespaceDetails : boost::noncopyable {
    public:
        static const int NIndexesMax = 64;

        // Flags for write operations. For performance reasons only. Use with caution.
        static const uint64_t NO_LOCKTREE = 1; // skip acquiring locktree row locks
        static const uint64_t NO_UNIQUE_CHECKS = 2; // skip uniqueness checks on all keys
        static const uint64_t KEYS_UNAFFECTED_HINT = 4; // an update did not update secondary indexes
        static const uint64_t NO_PK_UNIQUE_CHECKS = 8; // skip uniqueness checks only on the primary key

        // Creates the appropriate NamespaceDetails implementation based on options.
        static shared_ptr<NamespaceDetails> make(const StringData &ns, const BSONObj &options);
        // The bulkLoad parameter is used by beginBulkLoad to open an existing
        // IndexedCollection using a BulkLoadedCollection interface.
        static shared_ptr<NamespaceDetails> make(const BSONObj &serialized, const bool bulkLoad = false);

        virtual ~NamespaceDetails() {
        }

        const IndexPathSet &indexKeys() const {
            return _indexedPaths;
        }

        void clearQueryCache();

        /* you must notify the query cache if you are doing writes,
         * as the query plan utility may change */
        void notifyOfWriteOp();

        CachedQueryPlan cachedQueryPlanForPattern( const QueryPattern &pattern );

        void registerCachedQueryPlanForPattern( const QueryPattern &pattern,
                                                const CachedQueryPlan &cachedQueryPlan );

        class QueryCacheRWLock : boost::noncopyable {
        public:
            QueryCacheRWLock() : _lk("queryCache") { }
            struct Shared : boost::noncopyable {
                Shared(NamespaceDetails *d) : _lk(d->_qcRWLock._lk) { }
                SimpleRWLock::Shared _lk;
            };
            struct Exclusive : boost::noncopyable {
                Exclusive(NamespaceDetails *d) : _lk(d->_qcRWLock._lk) { }
                SimpleRWLock::Exclusive _lk;
            };
        private:
            SimpleRWLock _lk;
        } _qcRWLock;

        // Close the collection. For regular collections, closes the underlying IndexDetails
        // (and their underlying dictionaries). For bulk loaded collections, closes the
        // loader first and then closes dictionaries. The caller may wish to advise the 
        // implementation that the close() is getting called due to an aborting transaction.
        virtual void close(const bool aborting = false);

        // Ensure that the given index exists, or build it if it doesn't.
        // @param info is the index spec (ie: { ns: "test.foo", key: { a: 1 }, name: "a_1", clustering: true })
        // @return whether or the the index was just built.
        bool ensureIndex(const BSONObj &info);

        int nIndexes() const {
            return _nIndexes;
        }

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

        IndexDetails& idx(int idxNo) const;

        /** get the IndexDetails for the index currently being built in the background. (there is at most one) */
        IndexDetails& inProgIdx() const {
            dassert(_indexBuildInProgress);
            return idx(_nIndexes);
        }

        // TODO: replace with vector::iterator
        class IndexIterator {
        public:
            int pos() { return i; } // note this is the next one to come
            bool more() { return i < n; }
            IndexDetails& next() { return d->idx(i++); }
        private:
            friend class NamespaceDetails;
            int i, n;
            NamespaceDetails *d;
            IndexIterator(NamespaceDetails *_d);
        };

        IndexIterator ii() { return IndexIterator(this); }

        /* hackish - find our index # in the indexes array */
        int idxNo(const IndexDetails& idx) const;

        /* multikey indexes are indexes where there are more than one key in the index
             for a single document. see multikey in docs.
           for these, we have to do some dedup work on queries.
        */
        bool isMultikey(int i) const {
            const unsigned long long mask = 1ULL << i;
            return (_multiKeyIndexBits & mask) != 0;
        }
        void setIndexIsMultikey(const int idxNum);

        /**
         * Record that a new index exists in <dbname>.system.indexes.
         * Only used for the primary key index or an automatic _id index (capped collections),
         * the others go through the normal insert path.
         */
        void addDefaultIndexesToCatalog();

        // @return offset in indexes[]
        int findIndexByName(const StringData& name) const;

        // @return offset in indexes[]
        int findIndexByKeyPattern(const BSONObj& keyPattern) const;

        // @return the smallest (in terms of dataSize, which is key length + value length)
        //         index in _indexes that is one-to-one with the primary key. specifically,
        //         the returned index cannot be sparse or multikey.
        IndexDetails &findSmallestOneToOneIndex() const;

        /* Returns the index entry for the first index whose prefix contains
         * 'keyPattern'. If 'requireSingleKey' is true, skip indices that contain
         * array attributes. Otherwise, returns NULL.
         */
        const IndexDetails* findIndexByPrefix( const BSONObj &keyPattern ,
                                               bool requireSingleKey ) const;


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

        IndexDetails &getPKIndex() const {
            IndexDetails &idx = *_indexes[0];
            dassert(idx.keyPattern() == _pk);
            return idx;
        }

        // Key pattern for the primary key. For typical collections, this is { _id: 1 }.
        const BSONObj &pkPattern() const {
            return _pk;
        }

        bool indexBuildInProgress() const {
            return _indexBuildInProgress;
        }

        const string &ns() const {
            return _ns;
        }

        // @return a BSON representation of this NamespaceDetail's state
        static BSONObj serialize(const StringData& ns, const BSONObj &options,
                                 const BSONObj &pk, unsigned long long multiKeyIndexBits,
                                 const BSONArray &indexes_array);
        BSONObj serialize(const bool includeHotIndex = false) const;

        // struct for storing the accumulated states of a NamespaceDetails
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

        // Find the first object that matches the query. Force index if requireIndex is true.
        bool findOne(const BSONObj &query, BSONObj &result, const bool requireIndex = false) const;

        // Find by primary key (single element bson object, no field name).
        bool findByPK(const BSONObj &pk, BSONObj &result) const;

        // @return true, if fastupdates are ok for this collection.
        //         fastupdates are not ok for this collection if it's sharded
        //         and the primary key does not contain the full shard key.
        bool fastupdatesOk();

        // Extracts and returns validates an owned BSONObj represetning
        // the primary key portion of the given object. Validates each
        // field, ensuring there are no undefined, regex, or array types.
        virtual BSONObj getValidatedPKFromObject(const BSONObj &obj) const;

        // Extracts and returns an owned BSONObj representing
        // the primary key portion of the given query, if each
        // portion of the primary key exists in the query and
        // is 'simple' (ie: equality, no $ operators)
        virtual BSONObj getSimplePKFromQuery(const BSONObj &query) const;

        // send an optimize message into each index and run
        // hot optimize over all of the keys.
        virtual void optimizeAll();

        virtual void drop(string &errmsg, BSONObjBuilder &result, const bool mayDropSystem = false);
        virtual bool dropIndexes(const StringData& name, string &errmsg,
                                 BSONObjBuilder &result, bool mayDeleteIdIndex);
        
        // optional to implement, populate the obj builder with collection specific stats
        virtual void fillSpecificStats(BSONObjBuilder &result, int scale) const {
        }

        // return true if the namespace is currently under-going bulk load.
        virtual bool bulkLoading() const {
            return false;
        }

        // optional to implement, return true if the namespace is capped
        virtual bool isCapped() const {
            return false;
        }

        // inserts an object into this namespace, taking care of secondary indexes if they exist
        virtual void insertObject(BSONObj &obj, uint64_t flags = 0) = 0;

        // deletes an object from this namespace, taking care of secondary indexes if they exist
        virtual void deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags = 0);

        // update an object in the namespace by pk, replacing oldObj with newObj
        //
        // handles logging
        virtual void updateObject(const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj,
                                  const bool logop, const bool fromMigrate,
                                  uint64_t flags = 0);

        // update an object in the namespace by pk, described by the updateObj's $ operators
        //
        // handles logging
        virtual void updateObjectMods(const BSONObj &pk, const BSONObj &updateObj, 
                                      const bool logop, const bool fromMigrate,
                                      uint64_t flags = 0);

        // Interpret this NamespaceDetails as a subclass. Asserts if conversion fails.
        template <class T> T *as() {
            T *subclass = dynamic_cast<T *>(this);
            massert(17223, "bug: failed to dynamically cast NamespaceDetails to desired subclass", subclass != NULL);
            return subclass;
        }

        class Indexer : boost::noncopyable {
        public:
            // Prepare an index build. Must be write locked.
            //
            // Must ensure the given NamespaceDetails will remain valid for
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
            Indexer(NamespaceDetails *d, const BSONObj &info);
            // Must be write locked for destructor.
            virtual ~Indexer();

            // Indexer implementation specifics.
            virtual void _prepare() { }
            virtual void _commit() { }

            NamespaceDetails *_d;
            shared_ptr<IndexDetails> _idx;
            const BSONObj &_info;
            const bool _isSecondaryIndex;
        };

        // Indexer for background (aka hot, aka online) indexing.
        // build() should be called read locked, not write locked.
        class HotIndexer : public Indexer {
        public:
            HotIndexer(NamespaceDetails *d, const BSONObj &info);
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
        class ColdIndexer : public Indexer {
        public:
            ColdIndexer(NamespaceDetails *d, const BSONObj &info);
            virtual ~ColdIndexer() { }

            void build();
        };

    protected:
        NamespaceDetails(const StringData& ns, const BSONObj &pkIndexPattern, const BSONObj &options);
        explicit NamespaceDetails(const BSONObj &serialized);

        // create a new index with the given info for this namespace.
        virtual void createIndex(const BSONObj &info);
        void checkIndexUniqueness(const IndexDetails &idx);

        void insertIntoIndexes(const BSONObj &pk, const BSONObj &obj, uint64_t flags);
        void deleteFromIndexes(const BSONObj &pk, const BSONObj &obj, uint64_t flags);

        // uassert on duplicate key
        void checkUniqueIndexes(const BSONObj &pk, const BSONObj &obj);

        // generate an index info BSON for this namespace, with the same options
        BSONObj indexInfo(const BSONObj &keyPattern, bool unique, bool clustering) const;

        const string _ns;
        // The options used to create this namespace details. We serialize
        // this (among other things) to disk on close (see serialize())
        const BSONObj _options;
        // The primary index pattern.
        const BSONObj _pk;

        // State of fastupdates for sharding:
        // -1 not sure if fastupdates are okay - need to check if pk is in shardkey.
        // 0 fastupdates are deinitely not okay - sharding is enabled and pk is not in shardkey
        // 1 fastupdates are definitely okay - either no sharding, or the pk is in shardkey
        AtomicWord<int> _fastupdatesOkState;

        // Every index has an IndexDetails that describes it.
        bool _indexBuildInProgress;
        int _nIndexes;
        typedef std::vector<shared_ptr<IndexDetails> > IndexVector;
        IndexVector _indexes;

        unsigned long long _multiKeyIndexBits;

        void dropIndex(const int idxNum);

    private:
        IndexPathSet _indexedPaths;
        void resetTransient();
        void computeIndexKeys();

        /* query cache (for query optimizer) */
        int _qcWriteCount;
        map<QueryPattern, CachedQueryPlan> _qcCache;

        struct findByPKCallbackExtra {
            BSONObj &obj;
            std::exception *ex;
            findByPKCallbackExtra(BSONObj &o) : obj(o), ex(NULL) { }
        };
        static int findByPKCallback(const DBT *key, const DBT *value, void *extra);

        friend class CollectionMap;
    }; // NamespaceDetails

    NamespaceDetails *nsdetails(const StringData& ns);
    NamespaceDetails *nsdetails_maybe_create(const StringData& ns, BSONObj options = BSONObj());

    inline IndexDetails& NamespaceDetails::idx(int idxNo) const {
        verify( idxNo < NIndexesMax );
        verify( idxNo >= 0 && idxNo < (int) _indexes.size() );
        return *_indexes[idxNo];
    }

    inline int NamespaceDetails::idxNo(const IndexDetails& idx) const {
        for (IndexVector::const_iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
            const IndexDetails *index = it->get();
            if (index == &idx) {
                return it - _indexes.begin();
            }
        }
        msgasserted( 10349 , "E12000 idxNo fails" );
        return -1;
    }

    inline int NamespaceDetails::findIndexByKeyPattern(const BSONObj& keyPattern) const {
        for (IndexVector::const_iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
            const IndexDetails *index = it->get();
            if (index->keyPattern() == keyPattern) {
                return it - _indexes.begin();
            }
        }
        return -1;
    }

    inline IndexDetails &NamespaceDetails::findSmallestOneToOneIndex() const {
        // Default to choosing the primary key index (always at _indexes[0]);
        int chosenIndex = 0;

        // Check the secondary indexes. Any non-clustering secondary index is
        // better than using the primary key, since there's no object stored
        // and the key length can be at most the size of the object.
        uint64_t smallestIndexSize = std::numeric_limits<uint64_t>::max();
        for (int i = 1; i < _nIndexes; i++) {
            const IndexDetails *index = _indexes[i].get();
            IndexDetails::Stats st = index->getStats();
            if (!index->sparse() && !isMultikey(i) && st.dataSize < smallestIndexSize) {
                smallestIndexSize = st.dataSize;
                chosenIndex = i;
            }
        }

        return idx(chosenIndex);
    }

    inline const IndexDetails* NamespaceDetails::findIndexByPrefix( const BSONObj &keyPattern ,
                                                                    bool requireSingleKey ) const {
        const IndexDetails* bestMultiKeyIndex = NULL;
        for (IndexVector::const_iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
            const IndexDetails *index = it->get();
            if (keyPattern.isPrefixOf(index->keyPattern())) {
                if (!isMultikey(it - _indexes.begin())) {
                    return index;
                } else {
                    bestMultiKeyIndex = index;
                }
            }
        }
        return requireSingleKey ? NULL : bestMultiKeyIndex;
    }

    // @return offset in indexes[]
    inline int NamespaceDetails::findIndexByName(const StringData& name) const {
        for (IndexVector::const_iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
            const IndexDetails *index = it->get();
            if (index->indexName() == name) {
                return it - _indexes.begin();
            }
        }
        return -1;
    }

    inline NamespaceDetails::IndexIterator::IndexIterator(NamespaceDetails *_d) {
        d = _d;
        i = 0;
        n = d->nIndexes();
    }

} // namespace mongo
