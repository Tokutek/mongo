// namespace_details.h

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

#pragma once

#include <limits>

#include <db.h>
#include <boost/filesystem.hpp>

#include "mongo/pch.h"

#include "mongo/db/namespacestring.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/index.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace.h"
#include "mongo/db/queryoptimizercursor.h"
#include "mongo/db/querypattern.h"

#include "mongo/db/storage/env.h"

#include "mongo/util/hashtab.h"

namespace mongo {
    class Database;

    /** @return true if a client can modify this namespace even though it is under ".system."
        For example <dbname>.system.users is ok for regular clients to update.
        @param write used when .system.js
    */
    bool legalClientSystemNS( const string& ns , bool write );

    /* NamespaceDetails : this is the "header" for a namespace that has all its details.
       It is stored in the NamespaceIndex (a TokuDB dictionary named foo.ns, for Database foo).
    */
    class NamespaceDetails {
    public:
        enum { NIndexesMax = 64 };

        // TODO: Make these private
        //
        // TODO: TokuDB: Add in memory stats
        int nIndexes;
        // TODO: Use 'em or get rid of 'em;
        unsigned long long multiKeyIndexBits;
        // true if an index is currently being built
        bool indexBuildInProgress;

        //explicit NamespaceDetails( const DiskLoc &loc, bool _capped );
        explicit NamespaceDetails( bool _capped );

        /* called when loaded from disk */
        void onLoad(const Namespace& k);

        /* dump info on this namespace.  for debugging. */
        void dump(const Namespace& k);

        // TODO: Capped collections need are not yet implemented with TokuDB
        bool isCapped() const { return false; }
        long long maxCappedDocs() const { return std::numeric_limits<long long>::max(); }
        void setMaxCappedDocs( long long max ) { unimplemented("capped collections"); }
        void emptyCappedCollection(const char *ns);

        /* when a background index build is in progress, we don't count the index in nIndexes until
           complete, yet need to still use it in _indexRecord() - thus we use this function for that.
        */
        int nIndexesBeingBuilt() const { return nIndexes + (indexBuildInProgress ? 1 : 0); }

        IndexDetails& idx(int idxNo, bool missingExpected = false );

        /** get the IndexDetails for the index currently being built in the background. (there is at most one) */
        IndexDetails& inProgIdx() {
            DEV verify(indexBuildInProgress);
            return idx(nIndexes);
        }

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
        int idxNo(const IndexDetails& idx);

        /* multikey indexes are indexes where there are more than one key in the index
             for a single document. see multikey in wiki.
           for these, we have to do some dedup work on queries.
        */
        // TODO: Be sure to setIndexIsMultiKey on the insert path when we detect it.
        bool isMultikey(int i) const { return (multiKeyIndexBits & (((unsigned long long) 1) << i)) != 0; }
        void setIndexIsMultikey(const char *thisns, int i);

        /* add a new index.  does not add to system.indexes etc. - just to NamespaceDetails.
           caller must populate returned object.
         */
        IndexDetails& addIndex(const char *thisns, bool resetTransient=true);

        // @return offset in indexes[]
        int findIndexByName(const char *name);

        // @return offset in indexes[]
        int findIndexByKeyPattern(const BSONObj& keyPattern);

        void findIndexByType( const string& name , vector<int>& matches ) {
            IndexIterator i = ii();
            while ( i.more() ) {
                if ( i.next().getSpec().getTypeName() == name )
                    matches.push_back( i.pos() - 1 );
            }
        }

        /* Returns the index entry for the first index whose prefix contains
         * 'keyPattern'. If 'requireSingleKey' is true, skip indices that contain
         * array attributes. Otherwise, returns NULL.
         */
        const IndexDetails* findIndexByPrefix( const BSONObj &keyPattern ,
                                               bool requireSingleKey );


        /* @return -1 = not found
           generally id is first index, so not that expensive an operation (assuming present).
        */
        int findIdIndex() {
            IndexIterator i = ii();
            while( i.more() ) {
                if( i.next().isIdIndex() )
                    return i.pos()-1;
            }
            return -1;
        }

        // TODO: TokuDB-implemented namespaces always have an _id index. remove this.
        bool haveIdIndex() { 
            return true;
            //return isSystemFlagSet( NamespaceDetails::Flag_HaveIdIndex ) || findIdIndex() >= 0;
        }

        int averageObjectSize() {
            return 10; // TODO: Return something meaningful based on in-memory stats
        }

    private:
        // Each index (including the _id) index has an IndexDetails that describes it.
        IndexDetails _indexes[NIndexesMax];

        friend class NamespaceIndex;
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
        Database *database;
        const string _ns;
        void reset();
        static std::map< string, shared_ptr< NamespaceDetailsTransient > > _nsdMap;

        NamespaceDetailsTransient(Database*,const char *ns);
    public:
        ~NamespaceDetailsTransient();
        void addedIndex() { reset(); }
        void deletedIndex() { reset(); }
        /* Drop cached information on all namespaces beginning with the specified prefix.
           Can be useful as index namespaces share the same start as the regular collection.
           SLOW - sequential scan of all NamespaceDetailsTransient objects */
        static void clearForPrefix(const char *prefix);
        static void eraseForPrefix(const char *prefix);

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
         * @param simpleEqualityMatch - Set to true for certain simple queries - see
         * queryoptimizer.cpp.
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
         * The returned cursor may @throw inside of advance() or recoverFromYield() in certain error
         * cases, for example if a capped overrun occurred during a yield.  This indicates that the
         * cursor was unable to perform a complete scan.
         *
         * This is a work in progress.  Partial list of features not yet implemented through this
         * interface:
         * 
         * - covered indexes
         * - in memory sorting
         */
        static shared_ptr<Cursor> getCursor( const char *ns, const BSONObj &query,
                                            const BSONObj &order = BSONObj(),
                                            const QueryPlanSelectionPolicy &planPolicy =
                                            QueryPlanSelectionPolicy::any(),
                                            bool *simpleEqualityMatch = 0,
                                            const shared_ptr<const ParsedQuery> &parsedQuery =
                                            shared_ptr<const ParsedQuery>(),
                                            bool requireOrder = true,
                                            QueryPlanSummary *singlePlanSummary = 0 );

        /**
         * @return a single cursor that may work well for the given query.  A $or style query will
         * produce a single cursor, not a MultiCursor.
         * It is possible no cursor is returned if the sort is not supported by an index.  Clients are responsible
         * for checking this if they are not sure an index for a sort exists, and defaulting to a non-sort if
         * no suitable indices exist.
         */
        static shared_ptr<Cursor> bestGuessCursor( const char *ns, const BSONObj &query, const BSONObj &sort );

        /* indexKeys() cache ---------------------------------------------------- */
        /* assumed to be in write lock for this */
    private:
        bool _keysComputed;
        set<string> _indexKeys;
        void computeIndexKeys();
    public:
        /* get set of index keys for this namespace.  handy to quickly check if a given
           field is indexed (Note it might be a secondary component of a compound index.)
        */
        set<string>& indexKeys() {
            DEV Lock::assertWriteLocked(_ns);
            if ( !_keysComputed )
                computeIndexKeys();
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
        static NamespaceDetailsTransient& make_inlock(const char *ns);
    public:
        static SimpleMutex _qcMutex;

        /* you must be in the qcMutex when calling this.
           A NamespaceDetailsTransient object will not go out of scope on you if you are
           d.dbMutex.atLeastReadLocked(), so you do't have to stay locked.
           Creates a NamespaceDetailsTransient before returning if one DNE. 
           todo: avoid creating too many on erroneous ns queries.
           */
        static NamespaceDetailsTransient& get_inlock(const char *ns);

        static NamespaceDetailsTransient& get(const char *ns) {
            // todo : _qcMutex will create bottlenecks in our parallelism
            SimpleMutex::scoped_lock lk(_qcMutex);
            return get_inlock(ns);
        }

        void clearQueryCache() {
            _qcCache.clear();
            _qcWriteCount = 0;
        }
        /* you must notify the cache if you are doing writes, as query plan utility will change */
        // TODO: TokuDB: John does not understand why 100+ writes necessarily means
        // the query strategy changes. We need to figure this out eventually.
        void notifyOfWriteOp() {
            if ( _qcCache.empty() )
                return;
            if ( ++_qcWriteCount >= 100 )
                clearQueryCache();
        }
        CachedQueryPlan cachedQueryPlanForPattern( const QueryPattern &pattern ) {
            return _qcCache[ pattern ];
        }
        void registerCachedQueryPlanForPattern( const QueryPattern &pattern,
                                               const CachedQueryPlan &cachedQueryPlan ) {
            _qcCache[ pattern ] = cachedQueryPlan;
        }

    }; /* NamespaceDetailsTransient */

    inline NamespaceDetailsTransient& NamespaceDetailsTransient::get_inlock(const char *ns) {
        std::map< string, shared_ptr< NamespaceDetailsTransient > >::iterator i = _nsdMap.find(ns);
        if( i != _nsdMap.end() && 
            i->second.get() ) { // could be null ptr from clearForPrefix
            return *i->second;
        }
        return make_inlock(ns);
    }

    /* NamespaceIndex is the ".ns" file you see in the data directory.  It is the "system catalog"
       if you will: at least the core parts.  (Additional info in system.* collections.)
    */
    class NamespaceIndex {
    public:
        NamespaceIndex(const string &dir, const string &database) :
            ht( 0 ), dir_( dir ), database_( database ) {}

        /* returns true if new db will be created if we init lazily */
        // why
        bool exists() const;

        void init() {
            if( !ht ) 
                _init();
        }

        //void add_ns(const char *ns, DiskLoc& loc, bool capped);
        void add_ns( const char *ns, const NamespaceDetails &details );

        NamespaceDetails* details(const char *ns) {
            tokulog() << "looking for NamespaceDetails for " << ns << endl;
            if ( !ht )
                return 0;
            Namespace n(ns);
            NamespaceDetails *d = ht->get(n);
            if ( d && d->isCapped() ) {
                // What is the right thing to do here? //d->cappedCheckMigrate();
                unimplemented("capped collections");
            }
            if (d != NULL) {
                tokulog() << "found it" << endl;
            } else {
                tokulog() << "didn't find it" << endl;
            }
            return d;
        }

        void kill_ns(const char *ns);

        bool allocated() const { return ht != 0; }

        void getNamespaces( list<string>& tofill , bool onlyCollections = true ) const;

        boost::filesystem::path path() const;

        unsigned long long fileLength() const { unimplemented("NamespaceIndex::fileLength"); return 0; }

    private:
        void _init();

        DB *nsdb;
        HashTable<Namespace,NamespaceDetails> *ht;
        string dir_;
        string database_;
    };

    // Rename a namespace within current 'client' db.
    // (Arguments should include db name)
    void renameNamespace( const char *from, const char *to, bool stayTemp);

    // TODO: Put this in the cmdline abstraction, not extern global.
    extern string dbpath; // --dbpath parm

    // Defined in database.cpp
    // Gets the namespace objects for this client threads' current database.
    NamespaceIndex* nsindex(const char *ns);
    NamespaceDetails* nsdetails(const char *ns);
    NamespaceDetails* nsdetails_maybe_create(const char *ns);

} // namespace mongo

#include "namespace_details-inl.h"
