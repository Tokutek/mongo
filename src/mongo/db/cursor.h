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

namespace mongo {

    class NamespaceDetails;
    class CoveredIndexMatcher;

    extern BSONObj minKey;
    extern BSONObj maxKey;

    /**
     * Query cursors, base class.  This is for our internal cursors.  "ClientCursor" is a separate
     * concept and is for the user's cursor.
     *
     * WARNING concurrency: the vfunctions below are called back from within a
     * ClientCursor::ccmutex.  Don't cause a deadlock, you've been warned.
     */
    class Cursor : boost::noncopyable {
    public:
        virtual ~Cursor() {}
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

        virtual bool supportGetMore() = 0;

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

        /**
         * If true, this is an unindexed cursor over a capped collection.
         */
        virtual bool capped() const { return false; }

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
    };

    class FieldRange;
    class FieldRangeVector;
    class FieldRangeVectorIterator;
    struct FieldInterval;
    
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

        // Create a cursor over a specific start, end key range.
        static shared_ptr<IndexCursor> make( NamespaceDetails *d, const IndexDetails &idx,
                                             const BSONObj &startKey, const BSONObj &endKey,
                                             bool endKeyInclusive, int direction,
                                             int numWanted = 0);

        // Create a cursor over a set of one or more field ranges.
        static shared_ptr<IndexCursor> make( NamespaceDetails *d, const IndexDetails &idx,
                                             const shared_ptr< FieldRangeVector > &bounds,
                                             int singleIntervalLimit, int direction,
                                             int numWanted = 0);

        virtual ~IndexCursor();

        bool ok() { return _ok; }
        bool advance();
        bool supportGetMore() { return true; }

        /**
         * used for multikey index traversal to avoid sending back dups. see Matcher::matches() and cursor.h
         * @return false if the pk has not been seen
         */
        bool getsetdup(const BSONObj &pk) {
            if ( _multiKey ) {
                pair<set<BSONObj>::iterator, bool> p = _dups.insert(pk.copy());
                return !p.second;
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
        BSONObj prettyKey( const BSONObj &key ) const {
            return key.replaceFieldNames( indexKeyPattern() ).clientReadable();
        }

        CoveredIndexMatcher *matcher() const { return _matcher.get(); }
        void setMatcher( shared_ptr< CoveredIndexMatcher > matcher ) { _matcher = matcher;  }
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

        IndexCursor( NamespaceDetails *d, const IndexDetails &idx,
                     const BSONObj &startKey, const BSONObj &endKey,
                     bool endKeyInclusive, int direction, int numWanted = 0);

        IndexCursor( NamespaceDetails *d, const IndexDetails &idx,
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
        struct cursor_getf_extra {
            RowBuffer *buffer;
            int rows_fetched;
            int rows_to_fetch;
            std::exception *ex;
            cursor_getf_extra(RowBuffer *buf, int n_to_fetch) :
                buffer(buf), rows_fetched(0), rows_to_fetch(n_to_fetch), ex(NULL) {
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
        bool skipOutOfRangeKeysAndCheckEnd();
        void checkEnd();

        NamespaceDetails *const _d;
        const IndexDetails &_idx;
        const Ordering _ordering;

        set<BSONObj> _dups;
        BSONObj _startKey;
        BSONObj _endKey;
        BSONObj _minUnsafeKey;
        bool _endKeyInclusive;
        const bool _multiKey;
        const int _direction;
        shared_ptr< FieldRangeVector > _bounds; // field ranges to iterate over, if non-null
        auto_ptr< FieldRangeVectorIterator > _boundsIterator;
        shared_ptr< CoveredIndexMatcher > _matcher;
        shared_ptr<Projection::KeyOnly> _keyFieldsOnly;
        long long _nscanned;
        const int _numWanted;

        IndexDetails::Cursor _cursor;
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
    };

    /**
     * Abstracts index scans by generating a the start and end key
     * based on the index's ordering and the desired direction.
     */
    class IndexScanCursor : public IndexCursor {
    public:
        IndexScanCursor( NamespaceDetails *d, const IndexDetails &idx,
                         int direction, int numWanted = 0);
    private:
        static const BSONObj &startKey(const BSONObj &keyPattern, const int direction);
        static const BSONObj &endKey(const BSONObj &keyPattern, const int direction);
    };

    /**
     * Index-scan style cursor over the primary key.
     */
    class BasicCursor : public IndexScanCursor {
    public:
        static shared_ptr<Cursor> make( NamespaceDetails *d, int direction = 1 );

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
        BasicCursor( NamespaceDetails *d, int direction );
    };

    /**
     * Dummy cursor returning no results.
     * Can be used to represent a cursor over a non-existent collection.
     */
    class DummyCursor : public Cursor {
    public:
        DummyCursor( int direction = 1 ) : _direction(direction) { }
        bool ok() { return false; }
        BSONObj current() { return BSONObj(); }
        bool advance() { return false; }
        virtual string toString() const {
            return _direction > 0 ? "BasicCursor" : "ReverseCursor";
        }
        virtual bool getsetdup(const BSONObj &pk) { return false; }
        virtual bool isMultiKey() const { return false; }
        virtual bool modifiedKeys() const { return false; }
        virtual bool supportGetMore() { return true; }
        virtual void setMatcher( shared_ptr< CoveredIndexMatcher > matcher ) { }
        virtual void setKeyFieldsOnly( const shared_ptr<Projection::KeyOnly> &keyFieldsOnly ) { }
        virtual long long nscanned() const { return 0; }

    private:
        const int _direction;
    };
} // namespace mongo
