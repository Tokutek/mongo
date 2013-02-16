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

        virtual BSONObj indexKeyPattern() {
            return BSONObj();
        }

        virtual bool supportGetMore() = 0;

        virtual string toString() { return "abstract?"; }

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

        virtual long long nscanned() = 0;

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
        
        virtual void explainDetails( BSONObjBuilder& b ) { return; }
    };

    /**
     * table-scan style cursor
     *
     * A BasicCursor relies on advance() to ensure it is in a consistent state after a write.  If
     * the document at a BasicCursor's current position will be deleted or relocated, the cursor
     * must first be advanced.  The same is true of BasicCursor subclasses.
     */
    class BasicCursor : public Cursor {
    public:
        BasicCursor(NamespaceDetails *nsd, int direction = 1);
        ~BasicCursor();
        bool ok() {
            return !currKey().isEmpty();
        }
        BSONObj current() {
            return _currObj;
        }
        BSONObj currKey() const {
            return _currKey;
        };
        BSONObj currPK() const {
            // Basic cursors scan the _id index (the primary key),
            // so the current PK is just the current key.
            return currKey();
        };
        bool advance();
        virtual string toString() { return "BasicCursor"; }
        virtual void setTailable() {
#if 0
            if ( !curr.isNull() || !last.isNull() )
                _tailable = true;
#endif
            ::abort();
        }
        virtual bool tailable() { return _tailable; }
        virtual bool getsetdup(const BSONObj &pk) { return false; }
        virtual bool isMultiKey() const { return false; }
        virtual bool modifiedKeys() const { return false; }
        virtual bool supportGetMore() { return true; }
        virtual CoveredIndexMatcher *matcher() const { return _matcher.get(); }
        virtual shared_ptr< CoveredIndexMatcher > matcherPtr() const { return _matcher; }
        virtual void setMatcher( shared_ptr< CoveredIndexMatcher > matcher ) { _matcher = matcher; }
        virtual const Projection::KeyOnly *keyFieldsOnly() const { return _keyFieldsOnly.get(); }
        virtual void setKeyFieldsOnly( const shared_ptr<Projection::KeyOnly> &keyFieldsOnly ) {
            _keyFieldsOnly = keyFieldsOnly;
        }
        virtual long long nscanned() { return _nscanned; }

    protected:
        NamespaceDetails *_nsd;

        // TODO: _direction should be const
        int _direction;
        BSONObj _currKey;
        BSONObj _currObj;
        DBC *_cursor;
        void incNscanned() { if ( ok() ) { ++_nscanned; } }

    private:
        bool _tailable;
        shared_ptr< CoveredIndexMatcher > _matcher;
        shared_ptr<Projection::KeyOnly> _keyFieldsOnly;
        long long _nscanned;
        void init() { _tailable = false; }
    };

    /* used for order { $natural: -1 } */
    class ReverseCursor : public BasicCursor {
    public:
        ReverseCursor(NamespaceDetails *nsd) : BasicCursor(nsd, -1) { }
        virtual string toString() { return "ReverseCursor"; }
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
