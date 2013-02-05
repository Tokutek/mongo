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

#include "jsobj.h"
#include "diskloc.h"
#include "matcher.h"
#include "mongo/db/projection.h"

namespace mongo {

    class NamespaceDetails;
    //class Record;
    class CoveredIndexMatcher;

    /**
     * Query cursors, base class.  This is for our internal cursors.  "ClientCursor" is a separate
     * concept and is for the user's cursor.
     *
     * WARNING concurrency: the vfunctions below are called back from within a
     * ClientCursor::ccmutex.  Don't cause a deadlock, you've been warned.
     *
     * Two general techniques may be used to ensure a Cursor is in a consistent state after a write.
     *     - The Cursor may be advanced before the document at its current position is deleted.
     *     - The Cursor may record its position and then relocate this position.
     * A particular Cursor may potentially utilize only one of the above techniques, but a client
     * that is Cursor subclass agnostic must implement a pattern handling both techniques.
     *
     * When the document at a Cursor's current position is deleted (or moved to a new location) the
     * following pattern is used:
     *     DiskLoc toDelete = cursor->currLoc();
     *     while( cursor->ok() && cursor->currLoc() == toDelete ) {
     *         cursor->advance();
     *     }
     *     cursor->prepareToTouchEarlierIterate();
     *     delete( toDelete );
     *     cursor->recoverFromTouchingEarlierIterate();
     * 
     * When a cursor yields, the following pattern is used:
     *     cursor->prepareToYield();
     *     while( Op theOp = nextOp() ) {
     *         if ( theOp.type() == INSERT || theOp.type() == UPDATE_IN_PLACE ) {
     *             theOp.run();
     *         }
     *         else if ( theOp.type() == DELETE ) {
     *             if ( cursor->refLoc() == theOp.toDelete() ) {
     *                 cursor->recoverFromYield();
     *                 while ( cursor->ok() && cursor->refLoc() == theOp.toDelete() ) {
     *                     cursor->advance();
     *                 }
     *                 cursor->prepareToYield();
     *             }
     *             theOp.run();
     *         }
     *     }
     *     cursor->recoverFromYield();
     *     
     * The break before a getMore request is typically treated as a yield, but if a Cursor supports
     * getMore but not yield the following pattern is currently used:
     *     cursor->noteLocation();
     *     runOtherOps();
     *     cursor->checkLocation();
     *
     * But see SERVER-5725.
     *
     * A Cursor may rely on additional callbacks not listed above to relocate its position after a
     * write.
     *
     * XXX: TokuDB:
     * Everything mentioned above is more or less not applicable to our engine. We transactionally
     * handle * cursor consistency and have fine grained locking, so yielding doesn't do anything.
     */
    class Cursor : boost::noncopyable {
    public:
        virtual ~Cursor() {}
        virtual bool ok() = 0;
        bool eof() { return !ok(); }
        //virtual Record* _current() = 0;
        virtual BSONObj current() = 0;
        //virtual DiskLoc currLoc() = 0;
        virtual bool advance() = 0; /*true=ok*/
        virtual BSONObj currKey() const { return BSONObj(); }

        // DiskLoc the cursor requires for continued operation.  Before this
        // DiskLoc is deleted, the cursor must be incremented or destroyed.
        //virtual DiskLoc refLoc() = 0;

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
             if loc has already been sent, returns true.
             otherwise, marks loc as sent.
        */
        virtual bool getsetdup(DiskLoc loc) = 0;

        virtual bool isMultiKey() const = 0;

        virtual bool autoDedup() const { return true; }

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
         * If true, this is an unindexed cursor over a capped collection.  Currently such cursors must
         * not own a delegate ClientCursor, due to the implementation of ClientCursor::aboutToDelete(). - SERVER-4563
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

    // Cute, but not necessary
#if 0
    // strategy object implementing direction of traversal.
    class AdvanceStrategy {
    public:
        virtual ~AdvanceStrategy() { }
        //virtual DiskLoc next( const DiskLoc &prev ) const = 0;
    };

    const AdvanceStrategy *forward();
    const AdvanceStrategy *reverse();
#endif

    /**
     * table-scan style cursor
     *
     * A BasicCursor relies on advance() to ensure it is in a consistent state after a write.  If
     * the document at a BasicCursor's current position will be deleted or relocated, the cursor
     * must first be advanced.  The same is true of BasicCursor subclasses.
     */
    class BasicCursor : public Cursor {
    public:
        BasicCursor(/* DiskLoc dl , const AdvanceStrategy *_s = forward() */) : /* curr(dl), s( _s ), */ _nscanned() {
            incNscanned();
            init();
        }
#if 0
        BasicCursor( /*const AdvanceStrategy *_s = forward() */) : /*s( _s ), */ _nscanned() {
            init();
        }
#endif
        bool ok() { ::abort(); return false; /*return !curr.isNull();*/ }
#if 0
        Record* _current() {
            verify( ok() );
            return curr.rec();
        }
#endif
        BSONObj current() {
#if 0
            Record *r = _current();
            return BSONObj::make(r);
#endif
            ::abort(); return BSONObj();
        }
        //virtual DiskLoc currLoc() { return curr; }
        //virtual DiskLoc refLoc()  { return curr.isNull() ? last : curr; }
        bool advance();
        virtual string toString() { return "BasicCursor"; }
        virtual void setTailable() {
#if 0
            if ( !curr.isNull() || !last.isNull() )
                tailable_ = true;
#endif
            ::abort();
        }
        virtual bool tailable() { return tailable_; }
        virtual bool getsetdup(DiskLoc loc) { return false; }
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
        //DiskLoc curr, last;
        //const AdvanceStrategy *s;
        void incNscanned() { ::abort(); if ( /*!curr.isNull()*/ false ) { ++_nscanned; } }
    private:
        bool tailable_;
        shared_ptr< CoveredIndexMatcher > _matcher;
        shared_ptr<Projection::KeyOnly> _keyFieldsOnly;
        long long _nscanned;
        void init() { tailable_ = false; }
    };

    /* used for order { $natural: -1 } */
    class ReverseCursor : public BasicCursor {
    public:
        //ReverseCursor(/* DiskLoc dl */) : BasicCursor( /* dl, reverse() */ ) { }
        //ReverseCursor() : BasicCursor( /* reverse() */ ) { }
        virtual string toString() { return "ReverseCursor"; }
    };

    class ForwardCappedCursor : public BasicCursor /*, public AdvanceStrategy */ {
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
        NamespaceDetails *nsd;
    };

    class ReverseCappedCursor : public BasicCursor /*, public AdvanceStrategy */ {
    public:
        ReverseCappedCursor( NamespaceDetails *nsd = 0 /*, const DiskLoc &startLoc = DiskLoc() */ );
        virtual string toString() {
            return "ReverseCappedCursor";
        }
        //virtual DiskLoc next( const DiskLoc &prev ) const;
        virtual bool capped() const { return true; }
    private:
        NamespaceDetails *nsd;
    };

} // namespace mongo
