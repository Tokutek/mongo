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
#include "mongo/db/cursor.h"
#include "mongo/db/index.h"

namespace mongo {

    class FieldRangeVector;
    class FieldRangeVectorIterator;
    
    /**
     * A Cursor class for index iteration.
     */

    // TODO: TokuDB: A lot of btree/implementation-specific artifacts from vanilla mongo.
    // TODO: Clean house.
    class IndexCursor : public Cursor {
    protected:
        IndexCursor( NamespaceDetails* nsd , int theIndexNo, const IndexDetails& idxDetails );

        void init( const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction );
        void init( const shared_ptr< FieldRangeVector > &_bounds, int singleIntervalLimit, int _direction );

    private:
        void _finishConstructorInit();
        static IndexCursor* make( NamespaceDetails * nsd , int idxNo , const IndexDetails& indexDetails );

    public:
        ~IndexCursor();

        static IndexCursor* make( NamespaceDetails *_d, const IndexDetails&, const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction );
        static IndexCursor* make( NamespaceDetails *_d, const IndexDetails& _id, const shared_ptr< FieldRangeVector > &_bounds, int _direction );
        static IndexCursor* make( NamespaceDetails *_d, int _idxNo, const IndexDetails&, const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction );
        static IndexCursor* make( NamespaceDetails *_d, int _idxNo, const IndexDetails& _id,
                                 const shared_ptr< FieldRangeVector > &_bounds,
                                 int singleIntervalLimit, int _direction );

        bool ok() { ::abort(); return false; /* return !bucket.isNull(); */ }
        bool advance();
        bool supportGetMore() { return true; }

        /**
         * used for multikey index traversal to avoid sending back dups. see Matcher::matches().
         * if a multikey index traversal:
         *   if loc has already been sent, returns true.
         *   otherwise, marks loc as sent.
         * @return false if the loc has not been seen
         */
#if 0
        bool getsetdup(DiskLoc loc) {
            if( _multikey ) {
                pair<set<DiskLoc>::iterator, bool> p = _dups.insert(loc);
                return !p.second;
            }
            return false;
        }
#endif

        bool modifiedKeys() const { return _multikey; }
        bool isMultiKey() const { return _multikey; }

        BSONObj currKey() const;
        BSONObj indexKeyPattern() { return _order; }

        BSONObj current();
        string toString();

        BSONObj prettyKey( const BSONObj &key ) const {
            return key.replaceFieldNames( indexDetails.keyPattern() ).clientReadable();
        }

        BSONObj prettyIndexBounds() const;

        CoveredIndexMatcher *matcher() const { return _matcher.get(); }
        shared_ptr< CoveredIndexMatcher > matcherPtr() const { return _matcher; }

        void setMatcher( shared_ptr< CoveredIndexMatcher > matcher ) { _matcher = matcher;  }

        const Projection::KeyOnly *keyFieldsOnly() const { return _keyFieldsOnly.get(); }
        
        void setKeyFieldsOnly( const shared_ptr<Projection::KeyOnly> &keyFieldsOnly ) {
            _keyFieldsOnly = keyFieldsOnly;
        }
        
        long long nscanned() { return _nscanned; }

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
        void _audit() = 0;
        DiskLoc _locate(const BSONObj& key, const DiskLoc& loc) = 0;
        DiskLoc _advance(const DiskLoc& thisLoc, int& keyOfs, int direction, const char *caller) = 0;
        void _advanceTo(DiskLoc &thisLoc, int &keyOfs, const BSONObj &keyBegin, int keyBeginLen, bool afterKey, const vector< const BSONElement * > &keyEnd, const vector< bool > &keyEndInclusive, const Ordering &order, int direction ) = 0;

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
