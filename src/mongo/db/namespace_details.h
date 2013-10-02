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

    class NamespaceDetails;

    /** @return true if a client can modify this namespace even though it is under ".system."
        For example <dbname>.system.users is ok for regular clients to update.
        @param write used when .system.js
    */
    bool legalClientSystemNS( const StringData& ns , bool write );

    bool userCreateNS(const StringData& ns, BSONObj options, string& err, bool logForReplication);

    // used for operations that are supposed to create the namespace if it does not exist,
    // such as insert, some updates, and create index
    NamespaceDetails* getAndMaybeCreateNS(const StringData& ns, bool logop);

    void dropCollection(const StringData &name, string &errmsg, BSONObjBuilder &result, bool can_drop_system = false);

    void dropDatabase(const StringData &db);

    /**
     * Record that a new namespace exists in <dbname>.system.namespaces.
     */
    void addNewNamespaceToCatalog(const StringData& name, const BSONObj *options = NULL);

    void removeNamespaceFromCatalog(const StringData& name);

    void removeFromSysIndexes(const StringData& ns, const StringData& name);

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

    // struct for storing the accumulated states of a NamespaceDetails
    // all values, except for nIndexes, are estiamtes
    // note that the id index is used as the main store.
    struct NamespaceDetailsAccStats {
        uint64_t count; // number of rows in id index
        uint64_t size; // size of main store, which is the id index
        uint64_t storageSize; // size on disk of id index
        uint64_t nIndexes; // number of indexes, including id index
        uint64_t indexSize; // size of secondary indexes, NOT including id index
        uint64_t indexStorageSize; // size on disk for secondary indexes, NOT including id index
    };

    /* NamespaceDetails : this is the "header" for a namespace that has all its details.
       It is stored in the NamespaceIndex (a TokuMX dictionary named foo.ns, for Database foo).
    */
    class NamespaceDetails : boost::noncopyable {
    public:
        static const int NIndexesMax = 64;

        // Flags for write operations. For performance reasons only. Use with caution.
        static const uint64_t NO_LOCKTREE = 1; // skip acquiring locktree row locks
        static const uint64_t NO_UNIQUE_CHECKS = 2; // skip uniqueness checks

        // Creates the appropriate NamespaceDetails implementation based on options.
        static shared_ptr<NamespaceDetails> make(const StringData &ns, const BSONObj &options);
        // The bulkLoad parameter is used by beginBulkLoad to open an existing
        // IndexedCollection using a BulkLoadedCollection interface.
        static shared_ptr<NamespaceDetails> make(const BSONObj &serialized, const bool bulkLoad = false);

        virtual ~NamespaceDetails() {
        }

        const set<string> &indexKeys() const {
            return _indexKeys;
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

        // Acquire a full table lock on each index.
        void acquireTableLock();

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

        void fillCollectionStats(struct NamespaceDetailsAccStats* accStats, BSONObjBuilder* result, int scale) const;

        // Find the first object that matches the query. Force index if requireIndex is true.
        bool findOne(const BSONObj &query, BSONObj &result, const bool requireIndex = false) const;

        // Find by primary key (single element bson object, no field name).
        bool findByPK(const BSONObj &pk, BSONObj &result) const;

        // return true if this namespace has an index on the _id field.
        bool hasIdIndex() const {
            return findIdIndex() >= 0;
        }

        // send an optimize message into each index and run
        // hot optimize over all of the keys.
        virtual void optimizeAll();
        // @param left/rightPK [ left, right ] primary key range to run
        // hot optimize on. no optimize message is sent.
        virtual void optimizePK(const BSONObj &leftPK, const BSONObj &rightPK);

        virtual bool dropIndexes(const StringData& ns, const StringData& name, string &errmsg,
                                 BSONObjBuilder &result, bool mayDeleteIdIndex);

        virtual void validateConnectionId(const ConnectionId &id) {
            // By default, the calling connection id is valid.
            // Other implementations may decide otherwise.
        }
        
        // optional to implement, populate the obj builder with collection specific stats
        virtual void fillSpecificStats(BSONObjBuilder *result, int scale) const {
        }

        // optional to implement, return true if the namespace is capped
        virtual bool isCapped() const {
            return false;
        }

        // optional to implement, return the minimum key a tailable cursor
        // may not read (at the time of this call) to guaruntee that all keys
        // strictly less than the minUnsafeKey is either committed or aborted.
        virtual BSONObj minUnsafeKey() {
            massert(16864, "bug: should not call minUnsafeKey for collection that is not Oplog or capped", false);
        }

        // Hack for ops/query.cpp queryIdHack.
        // Lets us know if findById is okay to do. We should find a nicer way to do this eventually.
        // Even though a capped collection may have an _id index, it may not use the findById code path.
        virtual bool mayFindById() const {
            return false;
        }

        // finds an object by _id field
        virtual bool findById(const BSONObj &query, BSONObj &result) const {
            msgasserted( 16461, "findById shouldn't be called unless it is implemented." );
        }

        // inserts an object into this namespace, taking care of secondary indexes if they exist
        virtual void insertObject(BSONObj &obj, uint64_t flags = 0) = 0;

        // deletes an object from this namespace, taking care of secondary indexes if they exist
        virtual void deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags = 0);

        // update an object in the namespace by pk, replacing oldObj with newObj
        virtual void updateObject(const BSONObj &pk, const BSONObj &oldObj, BSONObj &newObj, uint64_t flags = 0);

        // remove everything from a collection
        virtual void empty();

        // note the commit/abort of a transaction, given:
        // minPK: the minimal PK inserted
        // nDelta: the number of inserts minus the number of deletes
        // sizeDelta: the size of inserts minus the size of deletes
        virtual void noteCommit(const BSONObj &minPK, long long nDelta, long long sizeDelta) {
            msgasserted( 16756, "bug: noted a commit, but it wasn't implemented" );
        }
        virtual void noteAbort(const BSONObj &minPK, long long nDelta, long long sizeDelta) {
            msgasserted( 16757, "bug: noted an abort, but it wasn't implemented" );
        }

        virtual void insertObjectIntoCappedAndLogOps(BSONObj &obj, uint64_t flags) {
            msgasserted( 16775, "bug: should not call insertObjectIntoCappedAndLogOps into non-capped collection" );
        }

        virtual void insertObjectIntoCappedWithPK(BSONObj& pk, BSONObj& obj, uint64_t flags) {
            msgasserted( 16772, "bug: should not call insertObjectIntoCappedWithPK into non-capped collection" );
        }
        
        virtual void deleteObjectFromCappedWithPK(BSONObj& pk, BSONObj& obj, uint64_t flags) {
            msgasserted( 16773, "bug: should not call deleteObjectFromCappedWithPK into non-capped collection" );
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

        // fill the statistics for each index in the NamespaceDetails,
        // indexStats is an array of length nIndexes
        void fillIndexStats(std::vector<IndexStats> &indexStats) const;

        const string _ns;
        // The options used to create this namespace details. We serialize
        // this (among other things) to disk on close (see serialize())
        const BSONObj _options;
        // The primary index pattern.
        const BSONObj _pk;

        // Each index (including the _id) index has an IndexDetails that describes it.
        bool _indexBuildInProgress;
        int _nIndexes;
        typedef std::vector<shared_ptr<IndexDetails> > IndexVector;
        IndexVector _indexes;

        unsigned long long _multiKeyIndexBits;

    protected:
        void dropIndex(const int idxNum);

    private:
        set<string> _indexKeys;
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

        friend class NamespaceIndex;
        friend class EmptyCapped; // for empty() only
    }; // NamespaceDetails

    /* NamespaceIndex is the the "system catalog" if you will: at least the core parts.
     * (Additional info in system.* collections.) */
    class NamespaceIndex {
    public:
        NamespaceIndex(const string &dir, const StringData& database);

        ~NamespaceIndex();

        void init(bool may_create = false);

        // @return true if the ns existed and was closed, false otherwise.
        bool close_ns(const StringData& ns, const bool aborting = false);

        // The index entry for ns is removed and brought up-to-date with the nsdb on txn abort.
        void add_ns(const StringData& ns, shared_ptr<NamespaceDetails> details);

        // The index entry for ns is removed and brought up-to-date with the nsdb on txn abort.
        void kill_ns(const StringData& ns);

        // If something changes that causes details->serialize() to be different,
        // call this to persist it to the nsdb.
        void update_ns(const StringData& ns, const BSONObj &serialized, bool overwrite);

        // Find an NamespaceDetails in the nsindex.
        // Will not open the if its closed, unlike nsdetails()
        NamespaceDetails *find_ns(const StringData& ns) {
            init();
            if (!allocated()) {
                return NULL;
            }

            SimpleRWLock::Shared lk(_openRWLock);
            return find_ns_locked(ns);
        }

        // Every namespace that exists has an entry in _namespaces. Some
        // entries may be "closed" in the sense that the key exists but the
        // value is null. If the desired namespace is closed, we open it,
        // which must succeed, by the first invariant.
        NamespaceDetails *details(const StringData& ns) {
            init();
            if (!allocated()) {
                return NULL;
            }

            {
                // Try to find the ns in a shared lock. If it's there, we're done.
                SimpleRWLock::Shared lk(_openRWLock);
                NamespaceDetails *d = find_ns_locked(ns);
                if (d != NULL) {
                    d->validateConnectionId(cc().getConnectionId());
                    return d;
                }
            }

            // The ns doesn't exist, or it's not opened.
            NamespaceDetails *d = open_ns(ns);
            if (d != NULL) {
                d->validateConnectionId(cc().getConnectionId());
            }
            return d;
        }

        bool allocated() const { return _nsdb; }

        void getNamespaces( list<string>& tofill );

        // drop all collections and the nsindex, we're removing this database
        void drop();

        void rollbackCreate();

        typedef StringMap<shared_ptr<NamespaceDetails> > NamespaceDetailsMap;

    private:
        int _openNsdb(bool may_create);
        void _init(bool may_create);

        // @return NamespaceDetails object is the ns is currently open, NULL otherwise.
        // requires: openRWLock is locked, either shared or exclusively.
        NamespaceDetails *find_ns_locked(const StringData& ns) {
            NamespaceDetailsMap::const_iterator it = _namespaces.find(ns);
            if (it != _namespaces.end()) {
                verify(it->second.get() != NULL);
                return it->second.get();
            }
            return NULL;
        }

        // @return NamespaceDetails object if the ns existed and is now open, NULL otherwise.
        // called with no locks held - synchronization is done internally.
        NamespaceDetails *open_ns(const StringData& ns, const bool bulkLoad = false);
        // Only beginBulkLoad may call open_ns with bulkLoad = true.
        friend void beginBulkLoad(const StringData &ns, const vector<BSONObj> &indexes, const BSONObj &options);

        NamespaceDetailsMap _namespaces;
        const string _dir;
        const string _nsdbFilename;
        const string _database;

        // The underlying ydb dictionary that stores namespace information.
        // - May not transition _nsdb from non-null to null in a DBRead lock.
        shared_ptr<storage::Dictionary> _nsdb;

        // It isn't necessary to hold either of these locks in a a DBWrite lock.

        // This lock protects access to the _namespaces variable
        // With a DBRead lock and this shared lock, one can retrieve
        // a NamespaceDetails that has already been opened
        SimpleRWLock _openRWLock;
    };

    // Gets the namespace objects for this client threads' current database.
    NamespaceIndex *nsindex(const StringData& ns);
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
