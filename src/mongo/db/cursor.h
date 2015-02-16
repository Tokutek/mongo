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

#include "mongo/pch.h"

#include "mongo/db/jsobj.h"
#include "mongo/db/matcher.h"
#include "mongo/db/projection.h"
#include "mongo/db/client.h"
#include "mongo/db/index.h"
#include "mongo/db/storage/key.h"
#include "mongo/db/collection.h"

namespace mongo {

    class Collection;
    class CoveredIndexMatcher;

    /**
     * Query cursors, base class.  This is for our internal cursors.  "ClientCursor" is a separate
     * concept and is for the user's cursor.
     *
     * WARNING concurrency: the vfunctions below are called back from within a
     * ClientCursor::ccmutex.  Don't cause a deadlock, you've been warned.
     */
    class Cursor : boost::noncopyable {
    public:

        // Make an appropriate cursor:
        //
        // - cl: if null, a dummy cursor returning no results is returned.
        // - direction: > 0 means forward, < 0 means reverse
        // - numWanted, N: 0 means unlimited, > 0 means limited to N, < 0 means in batches of N
        // - countCursor: if true, request a cursor optimized for counts.
        //                cannot call currKey()/current() on any such cursor.

        // table-scan
        static shared_ptr<Cursor> make(CollectionData *cd,
                                       const int direction,
                                       const bool countCursor);
        
        static shared_ptr<Cursor> make(Collection *cl,
                                       const int direction = 1,
                                       const bool countCursor = false);

        // index-scan
        static shared_ptr<Cursor> make(CollectionData *cd, const IndexDetails &idx,
                                        const int direction, 
                                        const bool countCursor);

        static shared_ptr<Cursor> make(Collection *cl, const IndexDetails &idx,
                                       const int direction = 1,
                                       const bool countCursor = false);

        // index range scan between start/end
        static shared_ptr<Cursor> make(CollectionData *cd, const IndexDetails &idx,
                                       const BSONObj &startKey, const BSONObj &endKey,
                                       const bool endKeyInclusive,
                                       const int direction, const int numWanted,
                                       const bool countCursor);

        static shared_ptr<Cursor> make(Collection *cl, const IndexDetails &idx,
                                       const BSONObj &startKey, const BSONObj &endKey,
                                       const bool endKeyInclusive,
                                       const int direction, const int numWanted = 0,
                                       const bool countCursor = false);

        // index range scan by field bounds
        static shared_ptr<Cursor> make(CollectionData *cd, const IndexDetails &idx,
                                        const shared_ptr<FieldRangeVector> &bounds,
                                        const int singleIntervalLimit,
                                        const int direction, const int numWanted,
                                        const bool countCursor);
        
        static shared_ptr<Cursor> make(Collection *cl, const IndexDetails &idx,
                                       const shared_ptr<FieldRangeVector> &bounds,
                                       const int singleIntervalLimit,
                                       const int direction, const int numWanted = 0,
                                       const bool countCursor = false);
        virtual ~Cursor() { }

        virtual bool ok() = 0;

        bool eof() { return !ok(); }

        /* current associated document for the cursor.
         * implementation may or may not perform another query to satisfy this call. */
        virtual BSONObj current() = 0;

        /* returns true if the cursor was able to advance, false otherwise */
        virtual bool advance() = 0;

        /* current key in the index. */
        virtual BSONObj currKey() const { return BSONObj(); }

        /* current associated primary key (_id key) for the document */
        virtual BSONObj currPK() const { return BSONObj(); }

        /* Implement these if you want the cursor to be "tailable" */

        /* Request that the cursor starts tailing after advancing past last record. */
        /* The implementation may or may not honor this request. */
        virtual void setTailable() {}
        /* indicates if tailing is enabled. */
        virtual bool tailable() const {
            return false;
        }

        virtual BSONObj indexKeyPattern() const {
            return BSONObj();
        }

        virtual string toString() const { return "abstract?"; }

        /* used for multikey index traversal to avoid sending back dups. see Matcher::matches().
           if a multikey index traversal:
             if primary key (ie: _id) has already been sent, returns true.
             otherwise, marks pk as sent.
        */
        virtual bool getsetdup(const BSONObj &pk) = 0;

        virtual bool isMultiKey() const = 0;

        /**
         * return true if the keys in the index have been modified from the main doc
         * if you have { a : 1 , b : [ 1 , 2 ] }
         * an index on { a : 1 } would not be modified
         * an index on { b : 1 } would be since the values of the array are put in the index
         *                       not the array
         */
        virtual bool modifiedKeys() const = 0;

        virtual BSONObj prettyIndexBounds() const { return BSONArray(); }

        virtual BSONObj prettyKey(const BSONObj &key) const {
            const BSONObj &idxKey = indexKeyPattern();
            return idxKey.isEmpty() ? key : key.replaceFieldNames(idxKey).clientReadable();
        }

        virtual long long nscanned() const = 0;

        // The implementation may return different matchers depending on the
        // position of the cursor.  If matcher() is nonzero at the start,
        // matcher() should be checked each time advance() is called.
        // Implementations which generate their own matcher should return this
        // to avoid a matcher being set manually.
        // Note that the return values differ subtly here

        // Used when we want fast matcher lookup
        virtual CoveredIndexMatcher *matcher() const { return 0; }

        virtual bool currentMatches( MatchDetails *details = 0 ) {
            return !matcher() || matcher()->matchesCurrent( this, details );
        }

        // A convenience function for setting the value of matcher() manually
        // so it may be accessed later.  Implementations which must generate
        // their own matcher() should assert here.
        virtual void setMatcher( shared_ptr< CoveredIndexMatcher > matcher ) {
            massert( 13285, "manual matcher config not allowed", false );
        }

        /** @return the covered index projector for the current iterate, if any. */
        virtual const Projection::KeyOnly *keyFieldsOnly() const { return 0; }

        /**
         * Manually set the value of keyFieldsOnly() so it may be accessed later.  Implementations
         * that generate their own keyFieldsOnly() must assert.
         */
        virtual void setKeyFieldsOnly( const shared_ptr<Projection::KeyOnly> &keyFieldsOnly ) {
            massert( 16159, "manual keyFieldsOnly config not allowed", false );
        }
        
        virtual void explainDetails( BSONObjBuilder& b ) const { return; }

        /// Should this cursor be destroyed when it's namespace is deleted
        virtual bool shouldDestroyOnNSDeletion() { return true; }
    };

    class FieldRange;
    class FieldRangeVector;
    class FieldRangeVectorIterator;
    struct FieldInterval;
    
    /** Helper class for deduping primary keys (_id keys) */
    class PKDupSet {
    public:
        /** @return true if dup, otherwise return false and insert. */
        bool getsetdup( const BSONObj &pk ) {
            pair<set<BSONObj>::iterator, bool> p = _dups.insert(pk.copy());
            return !p.second;
        }
    private:
        set<BSONObj> _dups;
    };

    // Class for storing rows bulk fetched from TokuMX
    class RowBuffer {
    public:
        RowBuffer();
        ~RowBuffer();

        bool ok() const;

        bool isGorged() const;

        void current(storage::Key &sKey, BSONObj &obj) const;

        // Append a key and obj onto the buffer 
        void append(const storage::Key &sKey, const BSONObj &obj);

        // moves the buffer to the next key/pk/obj
        // returns:
        //      true, the buffer has data, you may call current().
        //      false, the buffer has no more data. don't call current() until append()
        bool next();

        // empty the row buffer, resetting all data and internal positions
        // only reset it fields if there is something in the buffer.
        void empty();

    private:
        class HeaderBits {
        public:
            static const unsigned char hasPK = 1;
            static const unsigned char hasObj = 2;
        };

        // store rows in a buffer that has a "preferred size". if we need to 
        // fit more in the buf, then it's okay to go over. _size captures the
        // real size of the buffer.
        // _end_offset is where we will write new bytes for append(). it is
        // modified and advanced after the append.
        // _current_offset is where we will read for current(). it is modified
        // and advanced after a next()
        static const size_t _BUF_SIZE_PREFERRED = 128 * 1024;
        size_t _size;
        size_t _current_offset;
        size_t _end_offset;
        char *_buf;
    };

    /**
     * A Cursor class for index iteration.
     */
    class IndexCursor : public Cursor {
    public:

        virtual ~IndexCursor();

        bool ok() { return _ok; }
        bool advance();

        /**
         * used for multikey index traversal to avoid sending back dups. see Matcher::matches() and cursor.h
         * @return false if the pk has not been seen
         */
        bool getsetdup(const BSONObj &pk) {
            if ( _multiKey ) {
                return _dups.getsetdup(pk);
            }
            return false;
        }

        bool tailable() const { return _tailable; }
        void setTailable();

        bool modifiedKeys() const { return _multiKey; }
        bool isMultiKey() const { return _multiKey; }

        BSONObj currPK() const { return _currPK; }
        BSONObj currKey() const { return _currKey; }
        BSONObj current();
        BSONObj indexKeyPattern() const { return _idx.keyPattern(); }

        string toString() const;
        BSONObj prettyIndexBounds() const;

        CoveredIndexMatcher *matcher() const { return _matcher.get(); }
        void setMatcher( shared_ptr< CoveredIndexMatcher > matcher ) { _matcher = matcher;  }
        bool currentMatches( MatchDetails *details = NULL );
        const Projection::KeyOnly *keyFieldsOnly() const { return _keyFieldsOnly.get(); }
        void setKeyFieldsOnly( const shared_ptr<Projection::KeyOnly> &keyFieldsOnly ) {
            _keyFieldsOnly = keyFieldsOnly;
        }
        
        long long nscanned() const { return _nscanned; }

    protected:
        bool forward() const;

        // Optionally pre-acquire row locks for this cursor. cursor_flags() and
        // getf_flags() should be the correct flags to pass to the ydb, given the
        // implementation of prelock() (ie: DB_PRELOCKED if locks were grabbed).
        virtual void prelock();
        virtual int cursor_flags();
        virtual int getf_flags();

        // True if intuitive bounds-ordering for minKey and MaxKey is reverse for
        // this cursor, based on an index ordering and cursor direction.
        //
        // Example: intuitively, { min, max } describes the bounds for an index scan.
        // But we need to possibly reverse those bounds to { max, min }. Here are
        // the possible outcomes for ordering/direction combinations:
        // - Descending/forward, return true (reverse to { max, min })
        // - Ascending/reverse, return true.
        // - Descending/reverse, return false (stay with { min, max })
        // - Ascending/forward, return false.
        static bool reverseMinMaxBoundsOrder(const Ordering &ordering, const int direction);

        IndexCursor( CollectionData *cl, const IndexDetails &idx,
                     const BSONObj &startKey, const BSONObj &endKey,
                     bool endKeyInclusive, int direction, int numWanted = 0);

        IndexCursor( CollectionData *cl, const IndexDetails &idx,
                     const shared_ptr< FieldRangeVector > &bounds,
                     int singleIntervalLimit, int direction, int numWanted = 0);

    private:

        /** Initialize the internal DBC */
        void initializeDBC();
        void _prelockCompoundBounds(const int currentRange, vector<const FieldInterval *> &combo,
                                    BufBuilder &startKeyBuilder, BufBuilder &endKeyBuilder);
        void _prelockBounds();
        void _prelockRange(const BSONObj &startKey, const BSONObj &endKey);

        /** Get the current key/pk/obj from the row buffer and set _currKey/PK/Obj */
        void getCurrentFromBuffer();
        /** Advance the internal DBC, not updating nscanned or checking the key against our bounds. */
        void _advance();

        /** ydb cursor callback + flags */
        struct cursor_getf_extra : public ExceptionSaver {
            RowBuffer *buffer;
            int rows_fetched;
            int rows_to_fetch;
            cursor_getf_extra(RowBuffer *buf, int n_to_fetch) :
                buffer(buf), rows_fetched(0), rows_to_fetch(n_to_fetch) {
            }
        };
        static int cursor_getf(const DBT *key, const DBT *val, void *extra);
        /** determine how many rows the next getf should bulk fetch */
        int getf_fetch_count();
        /** pull more rows from the DBC into the RowBuffer */
        bool fetchMoreRows();
        /** find by key where the PK used for search is determined by _direction */
        void findKey(const BSONObj &key);
        /** find by key and a given PK */
        void setPosition(const BSONObj &key, const BSONObj &pk);
        /** check if the current key is out of bounds, invalidate the current key if so */
        bool checkCurrentAgainstBounds();
        void skipPrefix(const BSONObj &key, const int k);
        int skipToNextKey(const BSONObj &currentKey);
        /** for tailable cursors to get a fresh value for minUnsafeKey from a TailableCollection */
        void refreshMinUnsafeEndKey();

        static bool cursor_check_interrupt(void* extra, uint64_t deleted_rows);
        /**
         * Attempt to locate the next index key matching _bounds.  This may mean advancing to the
         * next successive key in the index, or skipping to a new position in the index.  If an
         * internal iteration cutoff is reached before a matching key is found, then the search for
         * a matching key will be aborted, leaving the cursor pointing at a key that is not within
         * bounds.
         */
        bool skipOutOfRangeKeysAndCheckEnd();
        void checkEnd();

    protected:
        CollectionData* _cl;
        const IndexDetails &_idx;
        const Ordering _ordering;

        PKDupSet _dups;
        BSONObj _startKey;
        BSONObj _endKey;
        BSONObj _minUnsafeKey;
        bool _endKeyInclusive;
        const bool _multiKey;
        const int _direction;
        shared_ptr< FieldRangeVector > _bounds; // field ranges to iterate over, if non-null
        auto_ptr< FieldRangeVectorIterator > _boundsIterator;
        bool _boundsMustMatch; // If iteration is aborted before a key matching _bounds is
                               // identified, the cursor may be left pointing at a key that is not
                               // within bounds (_bounds->matchesKey( currKey() ) may be false).
                               // _boundsMustMatch will be set to false accordingly.
        shared_ptr< CoveredIndexMatcher > _matcher;
        shared_ptr<Projection::KeyOnly> _keyFieldsOnly;
        long long _nscanned;
        long long _nscannedObjects;

        // Prelock is true if the caller does not want a limited result set from the cursor.
        // Even if the query looks like { a: { $gte: 5 } }, the caller may want limited results for:
        // - a delete has justOne = true
        // - an update has multi = false
        // - any findAndModify (since it's implemented as an update with multi = false)
        // The caller can let us know a limited result set is requested when all of these are true:
        // - the numWanted parameter is not zero (zero means "unlimited want", in the constructor)
        // - cc().opSettings().justOne() is true.
        const bool _prelock;

        shared_ptr<storage::Cursor> _cursor;
        // An exhausted cursor has no more rows and is done iterating,
        // unless it's tailable. If so, it may try to read more rows.
        bool _tailable;
        bool _ok;

        // The current key, pk, and obj for this cursor. Keys are stored
        // in a compacted format and built into bson format, so we reuse
        // a BufBuilder to prevent a malloc/free on each row read.
        BSONObj _currKey;
        BSONObj _currPK;
        BSONObj _currObj;
        BufBuilder _currKeyBufBuilder;

        // Row buffer to store rows in using bulk fetch. Also track the iteration
        // of bulk fetch so we know an appropriate amount of rows to fetch.
        RowBuffer _buffer;
        int _getf_iteration;

        // for interrupt checking
        ExceptionSaver _interrupt_extra;

        // For the Cursor::make() family of factories
        friend class CollectionBase;
    };

    /**
     * Abstracts index scans by generating a the start and end key
     * based on the index's ordering and the desired direction.
     */
    class ScanCursor : boost::noncopyable {
    protected:
        static const BSONObj &startKey(const BSONObj &keyPattern, const int direction);
        static const BSONObj &endKey(const BSONObj &keyPattern, const int direction);

    private:
        static bool reverseMinMaxBoundsOrder(const Ordering &ordering, const int direction);
    };

    /**
     * Cursor for scanning an index.
     */
    class IndexScanCursor : public IndexCursor, ScanCursor {
    protected:
        IndexScanCursor(CollectionData *cl, const IndexDetails &idx,
                        const int direction, const int numWanted = 0);

        // For the Cursor::make() family of factories
        friend class CollectionBase;
    };

    /**
     * Cursor optimized for count operations.
     * Does not track the current key or pk.
     * Cannot produce a meaningful result for current().
     */
    class IndexCountCursor : public IndexCursor {
    public:
        virtual string toString() const {
            return "IndexCountCursor";
        }

        BSONObj current() {
            msgasserted(17036, "bug: IndexCountCursor cannot return current()");
        }

        // A counting cursor can only be used when no matcher is necessary.
        bool currentMatches(MatchDetails *details = NULL) {
            dassert(matcher() == NULL);
            return true;
        }

        // A counting cursor cannot be used on multikey indexes, because that
        // would require tracking the current PK for manual deduplication.
        bool getsetdup(const BSONObj &pk) {
            dassert(!isMultiKey());
            return false;
        }

        void setMatcher(shared_ptr<CoveredIndexMatcher> matcher) {
            if (matcher) {
                msgasserted(17039, "bug: IndexCountCursor cannot utilize a matcher");
            }
        }

        struct count_cursor_getf_extra : public ExceptionSaver {
            count_cursor_getf_extra(int &c, bool &e, const storage::Key &key,
                                    const Ordering &o, const bool inc) :
                bufferedRowCount(c), exhausted(e), endSKeyPrefix(key), ordering(o), endKeyInclusive(inc) {
            }
            int &bufferedRowCount;
            bool &exhausted;
            const storage::Key &endSKeyPrefix;
            const Ordering &ordering;
            const bool endKeyInclusive;
        };
        static int count_cursor_getf(const DBT *key, const DBT *val, void *extra);

        bool advance();

    protected:
        IndexCountCursor( CollectionData *cl, const IndexDetails &idx,
                          const BSONObj &startKey, const BSONObj &endKey,
                          const bool endKeyInclusive );

        IndexCountCursor( CollectionData *cl, const IndexDetails &idx,
                          const shared_ptr< FieldRangeVector > &bounds );


    private:
        bool countMoreRows();
        void checkAssumptionsAndInit();

        // The number of rows counted during the last bulk-fetch
        int _bufferedRowCount; 
        // Whether the last bulk-fetch ended early because we reached an out-of-bounds key.
        bool _exhausted;
        // We only want to compare by the secondary key prefix.
        // This will be constructed with a NULL primary-key argument.
        const storage::Key _endSKeyPrefix;

        // For the Cursor::make() family of factories
        friend class CollectionBase;
    };

    /**
     * Cursor for scanning an index, optimized for counts.
     * See IndexScanCursor.
     */
    class IndexScanCountCursor : public IndexCountCursor, ScanCursor {
    private:
        IndexScanCountCursor( CollectionData *cl, const IndexDetails &idx );

        // For the Cursor::make() family of factories
        friend class CollectionBase;
    };

    /**
     * Index-scan style cursor over the primary key.
     */
    class BasicCursor : public IndexScanCursor {
    public:
        BSONObj currKey() const { return BSONObj(); }
        virtual BSONObj indexKeyPattern() const { return BSONObj(); }
        virtual string toString() const {
            return forward() ? "BasicCursor" : "ReverseCursor";
        }
        virtual bool getsetdup(const BSONObj &pk) { return false; }
        virtual bool isMultiKey() const { return false; }
        virtual bool modifiedKeys() const { return false; }
        virtual BSONObj prettyIndexBounds() const { return BSONArray(); }
        virtual void explainDetails( BSONObjBuilder& b ) const { return; }

    private:
        BasicCursor(CollectionData *cl, int direction);

        // For the Cursor::make() family of factories
        friend class CollectionBase;
    };

    /**
     * Dummy cursor returning no results. Used to represent a cursor over a
     * non-existent collection.
     */
    class DummyCursor : public Cursor {
    public:
        bool ok() { return false; }
        BSONObj current() { return BSONObj(); }
        bool advance() { return false; }
        virtual string toString() const {
            return _direction > 0 ? "BasicCursor" : "ReverseCursor";
        }
        virtual bool getsetdup(const BSONObj &pk) { return false; }
        virtual bool isMultiKey() const { return false; }
        virtual bool modifiedKeys() const { return false; }
        virtual void setMatcher( shared_ptr< CoveredIndexMatcher > matcher ) { }
        virtual void setKeyFieldsOnly( const shared_ptr<Projection::KeyOnly> &keyFieldsOnly ) { }
        virtual long long nscanned() const { return 0; }

    private:
        DummyCursor(int direction = 1) : _direction(direction) { }
        const int _direction;

        // For the Cursor::make() family of factories
        friend class Cursor;
    };

    //
    // a helper class for cursors that run over partitioned collections,
    // SinglePartitionCursorGenerator encapsulates the information needed by cursors
    // over partitioned collections (e.g. PartitionedCursor and SortedPartitionedCursor)
    // to generate cursors over the individual partitions.
    // This generator is needed because the implementations of these partitioned
    // cursors may need to generate new cursors for individual partitions at any time.
    //
    class SinglePartitionCursorGenerator {
    public:
        // generate a cursor on partition with index of partitionIndex
        shared_ptr<Cursor> makeSubCursor(uint64_t partitionIndex);
        virtual ~SinglePartitionCursorGenerator() { }
    protected:
        SinglePartitionCursorGenerator(
            PartitionedCollection* pc,
            const int idxNo,
            const int direction,
            const bool countCursor
            ) :
            _pc(pc),
            _idxNo(idxNo),
            _direction(direction),
            _countCursor(countCursor)
        {
        }
        virtual shared_ptr<Cursor> _makeSubCursor(uint64_t partitionIndex) = 0;
        const PartitionedCollection* _pc; // collection we are running cursor over
        const int _idxNo;
        // variables that all cursors use
        const int _direction;
        const bool _countCursor;
    };

    // a helper class for cursors that run over partitioned collections
    // This class gives the cursor a mechanism for identifying which
    // partitions the cursor needs to run over. The functions below
    // are to allow the cursor a way to iterate over the indexes
    class PartitionedCursorIDGenerator{
    public:
        virtual ~PartitionedCursorIDGenerator() { }
        // get the current partition index that this class is identifying
        virtual uint64_t getCurrentPartitionIndex() = 0;
        // advance from the current partition index the cursor cares
        // about to the next one. If lastIndex() is true, then this function
        // masserts
        virtual void advanceIndex() = 0;
        // return true if the current partition index is the final one
        // that the cursor cares about. If true, calls to advanceIndex
        // will massert
        virtual bool lastIndex() = 0;
    };

    // class for cursor over Partitioned Collection
    // This cursor iterates over the necessary partitions
    // one at a time returning results. That is, if we have
    // two partitions to iterate over, we iterate over the first
    // partition with user calls to advance() and current(),
    // and once we are done with the first partition, we move
    // on to the second. That means if the query is on an index
    // that is not the partition/primary key (as of this writing, they
    // are one and the same), then results may come out of order.
    // If results must be in order, the caller should use a SortedPartitionedCursor
    class PartitionedCursor : public Cursor {
    public:

        virtual bool ok() {
            return _currentCursor->ok();
        }

        virtual BSONObj current() {
            return _currentCursor->current();
        }

        virtual bool advance();

        virtual BSONObj currKey() const {
            return _currentCursor->currKey();
        }

        virtual BSONObj currPK() const {
            return _currentCursor->currPK();
        }

        virtual BSONObj indexKeyPattern() const {
            return _currentCursor->indexKeyPattern();
        }

        virtual string toString() const {
            if (_distributed) {
                return "DistributedPartitionedCursor";
            }
            return "PartitionedCursor";
        }

        virtual bool getsetdup(const BSONObj &pk) {
            if ( _multiKey ) {
                return _dups.getsetdup(pk);
            }
            return false;
        }

        virtual bool isMultiKey() const {
            return _multiKey;
        }

        virtual bool modifiedKeys() const {
            return _multiKey;
        }

        virtual BSONObj prettyIndexBounds() const {
            return _currentCursor->prettyIndexBounds();
        }

        virtual long long nscanned() const {
            return _prevNScanned + _currentCursor->nscanned();
        }

        virtual CoveredIndexMatcher *matcher() const {
            return _currentCursor->matcher();
        }

        virtual bool currentMatches( MatchDetails *details = 0 ) {
            return _currentCursor->currentMatches(details);
        }

        virtual void setMatcher( shared_ptr< CoveredIndexMatcher > matcher ) {
            _matcher = matcher;
            _currentCursor->setMatcher(matcher);
        }

        const Projection::KeyOnly *keyFieldsOnly() const { return _keyFieldsOnly.get(); }
        void setKeyFieldsOnly( const shared_ptr<Projection::KeyOnly> &keyFieldsOnly ) {
            _keyFieldsOnly = keyFieldsOnly;
            // unsure if this is necessary
            _currentCursor->setKeyFieldsOnly(keyFieldsOnly);
        }
        bool tailable() const { return _tailable; }
        void setTailable();

    private:
        PartitionedCursor(
            const bool distributed,
            shared_ptr<SinglePartitionCursorGenerator> subCursorGenerator,
            shared_ptr<PartitionedCursorIDGenerator> subPartitionIDGenerator,
            const bool multiKey
            );
        void getNextSubCursor();
        void initializeSubCursor();

        const bool _distributed;
        shared_ptr<SinglePartitionCursorGenerator> _subCursorGenerator;
        shared_ptr<PartitionedCursorIDGenerator> _partitionIDGenerator;
        const bool _multiKey;
        // cursor currently being used to retrieve documents
        shared_ptr<Cursor> _currentCursor;
        // number of documents scanned
        // by previous cursors
        long long _prevNScanned;        
        shared_ptr< CoveredIndexMatcher > _matcher;
        shared_ptr<Projection::KeyOnly> _keyFieldsOnly;

        bool _tailable;
        PKDupSet _dups;

        friend class PartitionedCollection;
    };

    // SPC stands for SortedPartitionedCursor, this is an element
    // of the vector we maintain in SortedPartitionedCursors::_cursors
    // first() is the cursor, second() is the partitionID, which is used
    // in comparisons
    typedef std::pair<shared_ptr<Cursor>, uint32_t > SPCSingleCursor;

    // Used within SortedPartitionedCursor to sort the heap of cursors
    class SPCComparator {
    public:
        SPCComparator(const int direction, const Ordering ordering) : _direction(direction), _ordering(ordering) {
        }
        // The top of the heap is what this operator reports as the "largest".
        // We want the top of the heap to be what the smallest value is, because
        // that is what the cursor should return next. Therefore, this function will
        // report the smallest value as "greater".
        bool operator()(const SPCSingleCursor &left, const SPCSingleCursor &right) const {
            shared_ptr<Cursor> leftCursor = left.first;
            shared_ptr<Cursor> rightCursor = right.first;
            uint32_t leftID = left.second;
            uint32_t rightID = right.second;
            if (!leftCursor->ok() && !rightCursor->ok()) {
                return leftID < rightID;
            }
            if (!leftCursor->ok()) {
                // say rightCursor is bigger
                return true;
            }
            else if (!rightCursor->ok()) {
                // say leftCursor is bigger
                return false;
            }
            // we want to say that the smaller one is "greater", so it goes to the top of the heap
            if (_direction > 0) {
                // if leftCursor < rightCursor, say leftCursor is bigger, so leftCursor gets put on top of heap
                // this is what we want for direction < 0
                if (rightCursor->currKey().woCompare(leftCursor->currKey(), _ordering) == 0) {
                    return (rightID < leftID);
                }
                return (rightCursor->currKey().woCompare(leftCursor->currKey(), _ordering) < 0);
            }
            // if leftCursor < rightCursor, say leftCursor is smaller, so rightCursor gets put on top of heap
            // this is what we want for direction < 0
            if (leftCursor->currKey().woCompare(rightCursor->currKey(), _ordering) == 0) {
                return (leftID < rightID);
            }
            return (leftCursor->currKey().woCompare(rightCursor->currKey(), _ordering) < 0);
        }
    private:
        const int _direction;
        const Ordering _ordering;
    };

    // a cursor over a partitioned collection that returns
    // elements in sorted order (ove the key we are querying)
    // That means we run cursors over all the necessary partitions
    // simultaneously
    class SortedPartitionedCursor : public Cursor {
    public:

        virtual bool ok() {
            return (frontCursor())->ok();
        }

        virtual BSONObj current() {
            return frontCursor()->current();
        }

        virtual bool advance();

        virtual BSONObj currKey() const {
            return frontCursor()->currKey();
        }

        virtual BSONObj currPK() const {
            return frontCursor()->currPK();
        }

        virtual BSONObj indexKeyPattern() const {
            return frontCursor()->indexKeyPattern();
        }

        virtual string toString() const {
            return "SortedPartitionedCursor";
        }

        virtual bool getsetdup(const BSONObj &pk) {
            if ( _multiKey ) {
                return _dups.getsetdup(pk);
            }
            return false;
        }

        virtual bool isMultiKey() const {
            return _multiKey;
        }

        virtual bool modifiedKeys() const {
            return _multiKey;
        }

        virtual BSONObj prettyIndexBounds() const {
            return frontCursor()->prettyIndexBounds();
        }

        virtual long long nscanned() const {
            long long ret = 0;
            for (vector<SPCSingleCursor>::const_iterator it = _cursors.begin(); it != _cursors.end(); it++) {
                ret += it->first->nscanned();
            }
            return ret;
        }

        virtual CoveredIndexMatcher *matcher() const {
            return _matcher.get();
        }

        virtual bool currentMatches( MatchDetails *details = 0 ) {
            return frontCursor()->currentMatches(details);
        }

        virtual void setMatcher( shared_ptr< CoveredIndexMatcher > matcher ) {
            _matcher = matcher;
            for (vector<SPCSingleCursor>::const_iterator it = _cursors.begin(); it != _cursors.end(); it++) {
                it->first->setMatcher(matcher);
            }
        }

        const Projection::KeyOnly *keyFieldsOnly() const { return _keyFieldsOnly.get(); }
        void setKeyFieldsOnly( const shared_ptr<Projection::KeyOnly> &keyFieldsOnly ) {
            _keyFieldsOnly = keyFieldsOnly;
            // unsure if this is necessary
            for (vector<SPCSingleCursor>::const_iterator it = _cursors.begin(); it != _cursors.end(); it++) {
                it->first->setKeyFieldsOnly(keyFieldsOnly);
            }
        }
        bool tailable() const { return false; }
        void setTailable() {
            uasserted(17348, "Cannot set a secondary index on a partitioned cursor to tailable");
        }

    private:
        SortedPartitionedCursor(
            const BSONObj idxPattern,
            const int direction,
            shared_ptr<SinglePartitionCursorGenerator> subCursorGenerator,
            shared_ptr<PartitionedCursorIDGenerator> subPartitionIDGenerator,
            const bool multiKey
            );
        const shared_ptr<Cursor> &frontCursor() const {
            return _cursors.front().first;
        }
        const int _direction;
        const Ordering _ordering;
        shared_ptr<SinglePartitionCursorGenerator> _subCursorGenerator;
        shared_ptr<PartitionedCursorIDGenerator> _partitionIDGenerator;
        const bool _multiKey;
        // must be declared after _direction and _ordering,
        // as it's initialization uses those variables
        SPCComparator _comparator;

        // number of documents scanned
        // by previous cursors
        shared_ptr< CoveredIndexMatcher > _matcher;
        shared_ptr<Projection::KeyOnly> _keyFieldsOnly;

        // cursors organized in a heap
        vector< SPCSingleCursor > _cursors;

        PKDupSet _dups;

        friend class PartitionedCollection;
    };

    // for range scans
    class RangePartitionCursorGenerator: public SinglePartitionCursorGenerator {
    public:
        RangePartitionCursorGenerator(
            PartitionedCollection* pc,
            const int idxNo,
            const int direction,
            const bool countCursor,
            const int numWanted,
            const BSONObj startKey,
            const BSONObj endKey,
            const bool endKeyInclusive
            ) :
            SinglePartitionCursorGenerator(pc, idxNo, direction, countCursor),
            _numWanted(numWanted),
            _startKey(startKey),
            _endKey(endKey),
            _endKeyInclusive(endKeyInclusive)
        {
        }
    protected:        
        virtual shared_ptr<Cursor> _makeSubCursor(uint64_t partitionIndex);
    private:
        const int _numWanted;
        const BSONObj _startKey;
        const BSONObj _endKey;
        const bool _endKeyInclusive;
    };

    // for scans that use bounds
    class BoundsPartitionCursorGenerator: public SinglePartitionCursorGenerator {
    public:
        BoundsPartitionCursorGenerator(
            PartitionedCollection* pc,
            const int idxNo,
            const int direction,
            const bool countCursor,
            const int numWanted,
            const shared_ptr<FieldRangeVector> bounds,
            const int singleIntervalLimit
            ) :
            SinglePartitionCursorGenerator(pc, idxNo, direction, countCursor),
            _numWanted(numWanted),
            _bounds(bounds),
            _singleIntervalLimit(singleIntervalLimit)
        {
        }
    protected:        
        virtual shared_ptr<Cursor> _makeSubCursor(uint64_t partitionIndex);
    private:
        const int _numWanted;
        const shared_ptr<FieldRangeVector> _bounds;
        const int _singleIntervalLimit;
    };

    // for full index/full collection scans
    class ExhaustivePartitionCursorGenerator: public SinglePartitionCursorGenerator {
    public:
        ExhaustivePartitionCursorGenerator(
            PartitionedCollection* pc,
            const int idxNo,
            const int direction,
            const bool countCursor,
            const bool cursorOverPartitionKey
            ) :
            SinglePartitionCursorGenerator(pc, idxNo, direction, countCursor),
            _cursorOverPartitionKey(cursorOverPartitionKey)
        {
        }
    protected:        
        virtual shared_ptr<Cursor> _makeSubCursor(uint64_t partitionIndex);
    private:
        const bool _cursorOverPartitionKey;
    };

    class PartitionedCursorIDGeneratorImpl : public PartitionedCursorIDGenerator {
    public:
        PartitionedCursorIDGeneratorImpl(PartitionedCollection* pc, const int direction);
        PartitionedCursorIDGeneratorImpl(
            PartitionedCollection* pc,
            const BSONObj &startKey,
            const BSONObj &endKey,
            const int direction
            );        
        PartitionedCursorIDGeneratorImpl(
            PartitionedCollection* pc,
            const shared_ptr<FieldRangeVector> &bounds,
            const int direction
            );
        virtual uint64_t getCurrentPartitionIndex();
        virtual void advanceIndex();
        virtual bool lastIndex();
    private:
        uint64_t _currPartition;
        const uint64_t _startPartition;
        const uint64_t _endPartition;
        const int _direction;
        void sanityCheckPartitionEndpoints();
    };

    class FilteredPartitionIDGeneratorImpl : public PartitionedCursorIDGenerator {
    public:
        FilteredPartitionIDGeneratorImpl(
            PartitionedCollection* pc,
            const char* ns,
            const ShardKeyPattern key,
            const int direction
            );
        virtual uint64_t getCurrentPartitionIndex();
        virtual void advanceIndex();
        virtual bool lastIndex();
    private:
        vector<bool> _partitionsToRead;
        uint64_t _currPartition;
        uint64_t _endPartition;
        const int _direction;
    };
} // namespace mongo
