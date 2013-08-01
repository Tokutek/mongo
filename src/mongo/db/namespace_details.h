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
#include "mongo/db/querypattern.h"
#include "mongo/db/relock.h"
#include "mongo/db/storage/env.h"
#include "mongo/util/concurrency/simplerwlock.h"
#include "mongo/util/string_map.h"

namespace mongo {

    class NamespaceDetails;
    class Database;

    // TODO: Put this in the cmdline abstraction, not extern global.
    extern string dbpath; // --dbpath parm

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

    int removeFromSysIndexes(const StringData& ns, const StringData& name);

    // Rename a namespace within current 'client' db.
    // (Arguments should include db name)
    void renameNamespace( const StringData& from, const StringData& to, bool stayTemp);

    // Manage bulk loading into a namespace
    //
    // To begin a load, the ns must exist and be empty.
    // TODO: Any other documentation to put here?
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
             for a single document. see multikey in wiki.
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

        void findIndexByType( const StringData& name , vector<int> &matches ) const {
            for (IndexVector::const_iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
                const IndexDetails *index = it->get();
                if (index->getSpec().getTypeName() == name) {
                    matches.push_back(it - _indexes.begin());
                }
            }
        }

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

        // @return a BSON representation of this NamespaceDetail's state
        static BSONObj serialize(const StringData& ns, const BSONObj &options,
                                 const BSONObj &pk, unsigned long long multiKeyIndexBits,
                                 const BSONArray &indexes_array);
        BSONObj serialize() const;

        void fillCollectionStats(struct NamespaceDetailsAccStats* accStats, BSONObjBuilder* result, int scale) const;

        // Find the first object that matches the query. Force index if requireIndex is true.
        bool findOne(const BSONObj &query, BSONObj &result, const bool requireIndex = false) const;

        // Find by primary key (single element bson object, no field name).
        bool findByPK(const BSONObj &pk, BSONObj &result) const;

        // return true if this namespace has an index on the _id field.
        bool hasIdIndex() const {
            return findIdIndex() >= 0;
        }

        // Run optimize on each index.
        virtual void optimize();

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
            return maxKey;
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
            // Must be write locked for both construction/destruction.
            Indexer(NamespaceDetails *d, const BSONObj &info);
            ~Indexer();

            // Must be read locked if the implementation is "hot" (since a write lock is overkill).
            //
            // Must be write locked while the implementation is still cold, which means
            // reads/writes to the collection are not allowed while the index is building.
            void build();

            // Must be write locked. If commit() succeeds (ie: does not throw), the
            // destructor must be called in the same write lock section to prevent
            // a race condition where another thread sets _indedBuildInProgress back
            // to true.
            void commit();

        private:
            // Need to ensure that this namespace will not get dropped/destroyed
            // during the lifetime of the indexer.
            NamespaceDetails *_d;
            const BSONObj _info;
            const bool _isSecondaryIndex;
        };

    protected:
        NamespaceDetails(const StringData& ns, const BSONObj &pkIndexPattern, const BSONObj &options);
        explicit NamespaceDetails(const BSONObj &serialized);

        // create a new index with the given info for this namespace.
        virtual void createIndex(const BSONObj &info);

        void checkIndexUniqueness(const IndexDetails &idx);
        void buildIndex(IndexDetails &idx);

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

    private:
        struct findByPKCallbackExtra {
            BSONObj &obj;
            std::exception *ex;
            findByPKCallbackExtra(BSONObj &o) : obj(o), ex(NULL) { }
        };
        static int findByPKCallback(const DBT *key, const DBT *value, void *extra);

        friend class NamespaceIndex;
        friend class EmptyCapped; // for empty() only
    }; // NamespaceDetails

    class ParsedQuery;
    class QueryPlanSummary;
    
    /* NamespaceDetailsTransient

       these are things we know / compute about a namespace that are transient -- things
       we don't actually store in the .ns file.  so mainly caching of frequently used
       information.

       CAUTION: Are you maintaining this properly on a collection drop()?  A dropdatabase()?  Be careful.
                The current field "allIndexKeys" may have too many keys in it on such an occurrence;
                as currently used that does not cause anything terrible to happen.

       todo: cleanup code, need abstractions and separation
    */
    // todo: multiple db's with the same name (repairDatbase) is not handled herein.  that may be 
    //       the way to go, if not used by repair, but need some sort of enforcement / asserts.
    class NamespaceDetailsTransient : boost::noncopyable {
        const string _ns;
        void reset();
        static StringMap<shared_ptr<NamespaceDetailsTransient> > _nsdMap;

        NamespaceDetailsTransient(const StringData& ns);
    public:
        ~NamespaceDetailsTransient();
        void addedIndex() { reset(); }
        void deletedIndex() { reset(); }
        /* Drop cached information on all namespaces beginning with the specified prefix.
           Can be useful as index namespaces share the same start as the regular collection.
           SLOW - sequential scan of all NamespaceDetailsTransient objects */
        static void clearForPrefix(const StringData& prefix);
        static void eraseForPrefix(const StringData& prefix);

        /**
         * @return a cursor interface to the query optimizer.  The implementation may utilize a
         * single query plan or interleave results from multiple query plans before settling on a
         * single query plan.  Note that the schema of currKey() documents, indexKeyPattern(), the
         * matcher(), and the isMultiKey() nature of the cursor may change over the course of
         * iteration.
         *
         * @param query - Query used to select indexes and populate matchers; not copied if unowned
         * (see bsonobj.h).
         *
         * @param order - Required ordering spec for documents produced by this cursor, empty object
         * default indicates no order requirement.  If no index exists that satisfies the required
         * sort order, an empty shared_ptr is returned unless parsedQuery is also provided.  This is
         * not copied if unowned.
         *
         * @param planPolicy - A policy for selecting query plans - see queryoptimizercursor.h
         *
         * @param requestMatcher - Set to true to request that the returned Cursor provide a
         * matcher().  If false, the cursor's matcher() may return NULL if the Cursor can perform
         * accurate query matching internally using a non Matcher mechanism.  One case where a
         * Matcher might be requested even though not strictly necessary to select matching
         * documents is if metadata about matches may be requested using MatchDetails.  NOTE This is
         * a hint that the Cursor use a Matcher, but the hint may be ignored.  In some cases the
         * returned cursor may not provide a matcher even if 'requestMatcher' is true.
         *
         * @param parsedQuery - Additional query parameters, as from a client query request.
         *
         * @param requireOrder - If false, the resulting cursor may return results in an order
         * inconsistent with the @param order spec.  See queryoptimizercursor.h for information on
         * handling these results properly.
         *
         * @param singlePlanSummary - Query plan summary information that may be provided when a
         * cursor running a single plan is returned.
         *
         * This is a work in progress.  Partial list of features not yet implemented through this
         * interface:
         * 
         * - covered indexes
         * - in memory sorting
         */
        static shared_ptr<Cursor> getCursor( const StringData& ns,
                                             const BSONObj& query,
                                             const BSONObj& order = BSONObj(),
                                             const QueryPlanSelectionPolicy& planPolicy =
                                                 QueryPlanSelectionPolicy::any(),
                                             bool requestMatcher = true,
                                             const shared_ptr<const ParsedQuery>& parsedQuery =
                                                 shared_ptr<const ParsedQuery>(),
                                             bool requireOrder = true,
                                             QueryPlanSummary* singlePlanSummary = NULL );

        /**
         * @return a single cursor that may work well for the given query.  A $or style query will
         * produce a single cursor, not a MultiCursor.
         * It is possible no cursor is returned if the sort is not supported by an index.  Clients are responsible
         * for checking this if they are not sure an index for a sort exists, and defaulting to a non-sort if
         * no suitable indices exist.
         */
        static shared_ptr<Cursor> bestGuessCursor( const StringData& ns, const BSONObj &query, const BSONObj &sort );

        /* indexKeys() cache ---------------------------------------------------- */
    private:
        bool _keysComputed;
        set<string> _indexKeys;
        /* throws RetryWithWriteLock if not write locked (writes to _indexKeys and _keysComputed) */
        void computeIndexKeys();
    public:
        /* get set of index keys for this namespace.  handy to quickly check if a given
           field is indexed (Note it might be a secondary component of a compound index.)
        */
        set<string>& indexKeys() {
            Lock::assertAtLeastReadLocked(_ns);
            if ( !_keysComputed ) {
                computeIndexKeys();
            }
            return _indexKeys;
        }

        /* IndexSpec caching */
    private:
        map<const IndexDetails*,IndexSpec> _indexSpecs;
        static SimpleMutex _isMutex;
    public:
        const IndexSpec& getIndexSpec( const IndexDetails * details ) {
            IndexSpec& spec = _indexSpecs[details];
            if ( ! spec._finishedInit ) {
                SimpleMutex::scoped_lock lk(_isMutex);
                if ( ! spec._finishedInit ) {
                    spec.reset( details );
                    verify( spec._finishedInit );
                }
            }
            return spec;
        }

        /* query cache (for query optimizer) ------------------------------------- */
    private:
        int _qcWriteCount;
        map<QueryPattern,CachedQueryPlan> _qcCache;
        static NamespaceDetailsTransient& make_inlock(const StringData& ns);
    public:
        static SimpleRWLock _qcRWLock;

        /* A NamespaceDetailsTransient object will not go out of scope on you if you are
           d.dbMutex.atLeastReadLocked(), so you do't have to stay locked.
           Creates a NamespaceDetailsTransient before returning if one DNE. 
           todo: avoid creating too many on erroneous ns queries.
           */
        static NamespaceDetailsTransient *_get(const StringData& ns);
        static NamespaceDetailsTransient &get_inlock(const StringData& ns);
        static NamespaceDetailsTransient &get(const StringData& ns) {
            {
                // Common path - try finding it with a shared lock.
                SimpleRWLock::Shared lk(_qcRWLock);
                NamespaceDetailsTransient *nsdt = _get(ns);
                if (nsdt != NULL) {
                    return *nsdt;
                }
            }
            // Try finding it again in an exclusive lock, creating if necessary.
            SimpleRWLock::Exclusive lk(_qcRWLock);
            return get_inlock(ns);
        }

        void clearQueryCache() {
            SimpleRWLock::Exclusive lk(_qcRWLock);
            _qcCache.clear();
            _qcWriteCount = 0;
        }

        /* you must notify the cache if you are doing writes, as query plan utility will change */
        void notifyOfWriteOp() {
            // Is there a problem calling empty() on a std::map without a lock?
            // What if other threads are doing deletes/inserts and a RB tree
            // child pointer changes to null or something?
            if ( _qcCache.empty() )
                return;
            if ( ++_qcWriteCount >= 100 )
                clearQueryCache();
        }
        CachedQueryPlan cachedQueryPlanForPattern( const QueryPattern &pattern ) {
            map<QueryPattern,CachedQueryPlan>::const_iterator i = _qcCache.find(pattern);
            return i != _qcCache.end() ? i->second : CachedQueryPlan();
        }
        void registerCachedQueryPlanForPattern( const QueryPattern &pattern,
                                                const CachedQueryPlan &cachedQueryPlan ) {
            _qcCache[ pattern ] = cachedQueryPlan;
        }

    }; /* NamespaceDetailsTransient */

    // does not create. must be at least shared locked.
    inline NamespaceDetailsTransient *NamespaceDetailsTransient::_get(const StringData& ns) {
        StringMap<shared_ptr<NamespaceDetailsTransient> >::const_iterator i = _nsdMap.find(ns);
        return i != _nsdMap.end() ? i->second.get() : NULL;
    }

    // creates if necessary. must be exclusive locked.
    inline NamespaceDetailsTransient& NamespaceDetailsTransient::get_inlock(const StringData& ns) {
        NamespaceDetailsTransient *nsdt = _get(ns);
        return nsdt != NULL ? *nsdt : make_inlock(ns);
    }

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

            // The ns doesn't exist, or it's not opened. Grab an exclusive lock
            // and do the open if we still can't find it.
            SimpleRWLock::Exclusive lk(_openRWLock);
            NamespaceDetails *d = find_ns_locked(ns);
            return d != NULL ? d->validateConnectionId(cc().getConnectionId()), d :
                               open_ns(ns);
        }

        bool allocated() const { return _nsdb != NULL; }

        void getNamespaces( list<string>& tofill );

        // drop all collections and the nsindex, we're removing this database
        void drop();

        void rollbackCreate();

        typedef StringMap<shared_ptr<NamespaceDetails> > NamespaceDetailsMap;

    private:
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
        // requires: _openRWLock is locked, exclusively.
        NamespaceDetails *open_ns(const StringData& ns, const bool bulkLoad = false);
        // Only beginBulkLoad may call open_ns with bulkLoad = true.
        friend void beginBulkLoad(const StringData &ns, const vector<BSONObj> &indexes, const BSONObj &options);

        DB *_nsdb;
        NamespaceDetailsMap _namespaces;
        const string _dir;
        const string _nsdbFilename;
        const string _database;
        // This rwlock serializes _nsdb/_namespaces opens and lookups in a DBRead lock.
        // It isn't necessary to hold this rwlock in a a DBWrite lock.
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
