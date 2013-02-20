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

#include "mongo/pch.h"

#include "mongo/db/jsobj.h"
#include "mongo/db/matcher.h"
#include "mongo/db/projection.h"
#include "mongo/db/client.h"
#include "mongo/db/index.h"

namespace mongo {

    class NamespaceDetails;
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
        virtual bool tailable() {
            return false;
        }

        /* optional to implement.  if implemented, means 'this' is a prototype */
        virtual Cursor* clone() {
            return 0;
        }

        virtual BSONObj indexKeyPattern() const {
            return BSONObj();
        }

        virtual bool supportGetMore() = 0;

        virtual string toString() const { return "abstract?"; }

        /* used for multikey index traversal to avoid sending back dups. see Matcher::matches().
           if a multikey index traversal:
             if primary key (ie: _id) has already been sent, returns true.
             otherwise, marks loc as sent.
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
        // Used when we need to share this matcher with someone else
        virtual shared_ptr< CoveredIndexMatcher > matcherPtr() const { return shared_ptr< CoveredIndexMatcher >(); }

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

    class FieldRangeVector;
    class FieldRangeVectorIterator;
    
    /**
     * A Cursor class for index iteration.
     */
    class IndexCursor : public Cursor {
    public:
        // Create a cursor over a specific start, end key range.
        IndexCursor( NamespaceDetails *d, const IndexDetails *idx,
                const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction );

        // Create a cursor over a set of one or more field ranges.
        IndexCursor( NamespaceDetails *d, const IndexDetails *idx,
                const shared_ptr< FieldRangeVector > &bounds, int singleIntervalLimit, int direction );

        ~IndexCursor();

        bool ok() { return !_currKey.isEmpty(); }
        bool advance();
        bool supportGetMore() { return true; }

        /**
         * used for multikey index traversal to avoid sending back dups. see Matcher::matches() and cursor.h
         * @return false if the pk has not been seen
         */
        bool getsetdup(const BSONObj &pk) {
            if( _multiKey ) {
                pair<set<BSONObj>::iterator, bool> p = _dups.insert(pk);
                return !p.second;
            }
            return false;
        }

        bool modifiedKeys() const { return _multiKey; }
        bool isMultiKey() const { return _multiKey; }

        BSONObj currPK() const { return _currPK; }
        BSONObj currKey() const { return _currKey; }
        BSONObj indexKeyPattern() const { return _idx != NULL ? _idx->keyPattern() : BSONObj(); }

        BSONObj current();
        string toString() const;

        BSONObj prettyKey( const BSONObj &key ) const {
            return key.replaceFieldNames( indexKeyPattern() ).clientReadable();
        }

        BSONObj prettyIndexBounds() const;

        CoveredIndexMatcher *matcher() const { return _matcher.get(); }
        shared_ptr< CoveredIndexMatcher > matcherPtr() const { return _matcher; }

        void setMatcher( shared_ptr< CoveredIndexMatcher > matcher ) { _matcher = matcher;  }

        const Projection::KeyOnly *keyFieldsOnly() const { return _keyFieldsOnly.get(); }
        
        void setKeyFieldsOnly( const shared_ptr<Projection::KeyOnly> &keyFieldsOnly ) {
            _keyFieldsOnly = keyFieldsOnly;
        }
        
        long long nscanned() const { return _nscanned; }

    private:
        void init( const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction );
        void init( const shared_ptr< FieldRangeVector > &_bounds, int singleIntervalLimit, int _direction );

        /** setup DBC cursor and its initial position */
        void initializeDBC();
        /** check if the current key is out of bounds, invalidate the current key if so */
        bool checkCurrentAgainstBounds();
        bool skipOutOfRangeKeysAndCheckEnd();
        void checkEnd();

        // If the namespace is does not exist and needs to be treated as empty,
        // then both _d and _idx will be null.
        NamespaceDetails * const _d;
        const IndexDetails *_idx;

        set<BSONObj> _dups;
        BSONObj _startKey;
        BSONObj _endKey;
        bool _endKeyInclusive;
        bool _multiKey; // this must be updated every getmore batch in case someone added a multikey
        int _direction;
        shared_ptr< FieldRangeVector > _bounds; // field ranges to iterate over, if non-null
        auto_ptr< FieldRangeVectorIterator > _boundsIterator;
        shared_ptr< CoveredIndexMatcher > _matcher;
        shared_ptr<Projection::KeyOnly> _keyFieldsOnly;
        long long _nscanned;

        // For primary _id index:
        //  _currKey == _currPK == actual dictionary key
        //  _currObj == full document, actual dictionary val
        // For secondary indexes:
        //  _currKey == secondary key data
        //  _currPK == associated primary key data
        //  _currKey + _currPK == actual dictionary key
        //  _currObj == full document
        //      actual dictionary val if clustering
        //      empty dictionary val otherwise, full document queried from _id index.
        BSONObj _currKey;
        BSONObj _currPK;
        BSONObj _currObj;

        DBC *_cursor;
    };

    /**
     * Table-scan style cursor.
     *
     * Implements the cursor interface by wrapping an IndexCursor
     * constructed over the primary, clustering _id index.
     */
    class BasicCursor : public Cursor {
    public:
        BasicCursor(NamespaceDetails *d, int direction = 1);
        ~BasicCursor() { }
        bool ok() { return _c.ok(); }
        BSONObj current() { return _c.current(); }
        BSONObj currPK() const { return _c.currPK(); }
        bool advance() { return _c.advance(); }
        virtual string toString() const { return "BasicCursor"; }
        virtual void setTailable() { _c.setTailable(); }
        virtual bool tailable() { return _c.tailable(); }
        virtual bool getsetdup(const BSONObj &pk) { return false; }
        virtual bool isMultiKey() const { return false; }
        virtual bool modifiedKeys() const { return false; }
        virtual bool supportGetMore() { return true; }
        virtual CoveredIndexMatcher *matcher() const { return _c.matcher(); }
        virtual shared_ptr< CoveredIndexMatcher > matcherPtr() const { return _c.matcherPtr(); }
        virtual void setMatcher( shared_ptr< CoveredIndexMatcher > matcher ) {
            _c.setMatcher(matcher);
        }
        virtual const Projection::KeyOnly *keyFieldsOnly() const { return _c.keyFieldsOnly(); }
        virtual void setKeyFieldsOnly( const shared_ptr<Projection::KeyOnly> &keyFieldsOnly ) {
            _c.setKeyFieldsOnly(keyFieldsOnly);
        }
        virtual long long nscanned() const { return _c.nscanned(); }

    private:
        IndexCursor _c;
    };

    /* used for order { $natural: -1 } */
    class ReverseCursor : public BasicCursor {
    public:
        ReverseCursor(NamespaceDetails *nsd) : BasicCursor(nsd, -1) { }
        virtual string toString() const { return "ReverseCursor"; }
    };

    // TODO: Capped collections
#if 0
    class ForwardCappedCursor : public BasicCursor {
    public:
        static ForwardCappedCursor* make( NamespaceDetails* nsd = 0 /*, const DiskLoc& startLoc = DiskLoc()*/ );
        virtual string toString() {
            return "ForwardCappedCursor";
        }
        //virtual DiskLoc next( const DiskLoc &prev ) const;
        virtual bool capped() const { return true; }
    private:
        ForwardCappedCursor( NamespaceDetails* nsd );
        //void init( const DiskLoc& startLoc );
    };

    class ReverseCappedCursor : public BasicCursor {
    public:
        ReverseCappedCursor( NamespaceDetails *nsd = 0 /*, const DiskLoc &startLoc = DiskLoc() */ );
        virtual string toString() {
            return "ReverseCappedCursor";
        }
        //virtual DiskLoc next( const DiskLoc &prev ) const;
        virtual bool capped() const { return true; }
    };
#endif

} // namespace mongo
