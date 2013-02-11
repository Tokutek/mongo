// indexcursor.h

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
#include "mongo/db/key.h"
#include "mongo/db/cursor.h"
#include "mongo/db/index.h"

namespace mongo {

    class FieldRangeVector;
    class FieldRangeVectorIterator;
    
    /**
     * A Cursor class for Btree iteration.
     * XXX That means "index" iteration.
     *
     * A IndexCursor can record its current btree position (noteLoc()) and then relocate this
     * position after a write (checkLoc()).  A recorded btree position consists of a btree bucket,
     * bucket key offset, and unique btree key.  To relocate a unique btree key, a IndexCursor first
     * checks the btree key at its recorded btree bucket and bucket key offset.  If the key at that
     * location does not match the recorded btree key, and an adjacent key also fails to match,
     * the recorded key (or the next existing key following it) is located in the btree using binary
     * search.  If the recorded btree bucket is invalidated, the initial recorded bucket check is
     * not attempted (see SERVER-4575).
     */

    // XXX TokuDB: We're going to want to de-virtualize this and rename it to IndexCursor, or something.
    class IndexCursor : public Cursor {
        // XXX: TokuDB: A lot of stuff in here has to do with their specific Btree implementation. Clean house.
    protected:
        IndexCursor( NamespaceDetails* nsd , int theIndexNo, const IndexDetails& idxDetails );

        virtual void init( const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction );
        virtual void init( const shared_ptr< FieldRangeVector > &_bounds, int singleIntervalLimit, int _direction );

    private:
        void _finishConstructorInit();
        static IndexCursor* make( NamespaceDetails * nsd , int idxNo , const IndexDetails& indexDetails );

    public:
        virtual ~IndexCursor();
        /** makes an appropriate subclass depending on the index version */
        static IndexCursor* make( NamespaceDetails *_d, const IndexDetails&, const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction );
        static IndexCursor* make( NamespaceDetails *_d, const IndexDetails& _id, const shared_ptr< FieldRangeVector > &_bounds, int _direction );
        static IndexCursor* make( NamespaceDetails *_d, int _idxNo, const IndexDetails&, const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction );
        static IndexCursor* make( NamespaceDetails *_d, int _idxNo, const IndexDetails& _id,
                                 const shared_ptr< FieldRangeVector > &_bounds,
                                 int singleIntervalLimit, int _direction );

        virtual bool ok() { ::abort(); return false; /* return !bucket.isNull(); */ }
        virtual bool advance();
        virtual bool supportGetMore() { return true; }

        /**
         * used for multikey index traversal to avoid sending back dups. see Matcher::matches().
         * if a multikey index traversal:
         *   if loc has already been sent, returns true.
         *   otherwise, marks loc as sent.
         * @return false if the loc has not been seen
         */
#if 0
        virtual bool getsetdup(DiskLoc loc) {
            if( _multikey ) {
                pair<set<DiskLoc>::iterator, bool> p = _dups.insert(loc);
                return !p.second;
            }
            return false;
        }
#endif

        virtual bool modifiedKeys() const { return _multikey; }
        virtual bool isMultiKey() const { return _multikey; }

        virtual BSONObj currKey() const = 0;
        virtual BSONObj indexKeyPattern() { return _order; }

        //virtual DiskLoc currLoc() = 0; //  return !bucket.isNull() ? _currKeyNode().recordLoc : DiskLoc();
        //virtual DiskLoc refLoc()   { return currLoc(); }
        virtual BSONObj current()  { ::abort(); return BSONObj(); } //return BSONObj::make(_current());
        virtual string toString();

        BSONObj prettyKey( const BSONObj &key ) const {
            return key.replaceFieldNames( indexDetails.keyPattern() ).clientReadable();
        }

        virtual BSONObj prettyIndexBounds() const;

        virtual CoveredIndexMatcher *matcher() const { return _matcher.get(); }
        virtual shared_ptr< CoveredIndexMatcher > matcherPtr() const { return _matcher; }

        virtual void setMatcher( shared_ptr< CoveredIndexMatcher > matcher ) { _matcher = matcher;  }

        virtual const Projection::KeyOnly *keyFieldsOnly() const { return _keyFieldsOnly.get(); }
        
        virtual void setKeyFieldsOnly( const shared_ptr<Projection::KeyOnly> &keyFieldsOnly ) {
            _keyFieldsOnly = keyFieldsOnly;
        }
        
        virtual long long nscanned() { return _nscanned; }

    protected:
        // John thinks this means skip any keys that are not
        // contained between two adjancet field ranges
        // in this cursor's field range vector.
        bool skipOutOfRangeKeysAndCheckEnd();
        void skipAndCheck();
        void checkEnd();

        /** selective audits on construction */
        void audit();

#if 0
        virtual void _audit() = 0;
        virtual DiskLoc _locate(const BSONObj& key, const DiskLoc& loc) = 0;
        virtual DiskLoc _advance(const DiskLoc& thisLoc, int& keyOfs, int direction, const char *caller) = 0;
        virtual void _advanceTo(DiskLoc &thisLoc, int &keyOfs, const BSONObj &keyBegin, int keyBeginLen, bool afterKey, const vector< const BSONElement * > &keyEnd, const vector< bool > &keyEndInclusive, const Ordering &order, int direction ) = 0;

#endif

        /** set initial bucket */
        void initWithoutIndependentFieldRanges();

        /** if afterKey is true, we want the first key with values of the keyBegin fields greater than keyBegin */
        void advanceTo( const BSONObj &keyBegin, int keyBeginLen, bool afterKey, const vector< const BSONElement * > &keyEnd, const vector< bool > &keyEndInclusive );

        // these are set in the construtor
        NamespaceDetails * const d;
        const int idxNo;
        const IndexDetails& indexDetails;

        // these are all set in init()
        //set<DiskLoc> _dups;
        BSONObj startKey;
        BSONObj endKey;
        bool _endKeyInclusive;
        bool _multikey; // this must be updated every getmore batch in case someone added a multikey
        BSONObj _order; // this is the same as indexDetails.keyPattern()
        Ordering _ordering;
        //DiskLoc bucket;
        int _direction; // 1=fwd,-1=reverse
        shared_ptr< FieldRangeVector > _bounds;
        auto_ptr< FieldRangeVectorIterator > _boundsIterator;
        shared_ptr< CoveredIndexMatcher > _matcher;
        shared_ptr<Projection::KeyOnly> _keyFieldsOnly;
        bool _independentFieldRanges;
        long long _nscanned;
    };

} // namespace mongo;
