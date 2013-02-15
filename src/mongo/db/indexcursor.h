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
#include "mongo/db/client.h"
#include "mongo/db/storage/env.h"

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
        static IndexCursor* make( NamespaceDetails * nsd , int idxNo , const IndexDetails& _idx );

    public:
        ~IndexCursor();

        static IndexCursor* make( NamespaceDetails *_d, const IndexDetails& idx, const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction );
        static IndexCursor* make( NamespaceDetails *_d, const IndexDetails& idx, const shared_ptr< FieldRangeVector > &bounds, int direction );
        static IndexCursor* make( NamespaceDetails *_d, int idxNo, const IndexDetails& idx, const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction );
        static IndexCursor* make( NamespaceDetails *_d, int idxNo, const IndexDetails& idx, const shared_ptr< FieldRangeVector > &bounds, int singleIntervalLimit, int direction );

        bool ok() { return !_currKey.isEmpty(); }
        bool advance();
        bool supportGetMore() { return true; }

        /**
         * used for multikey index traversal to avoid sending back dups. see Matcher::matches() and cursor.h
         * @return false if the pk has not been seen
         */
        bool getsetdup(const BSONObj &pk) {
            if( _multikey ) {
                pair<set<BSONObj>::iterator, bool> p = _dups.insert(pk);
                return !p.second;
            }
            return false;
        }

        bool modifiedKeys() const { return _multikey; }
        bool isMultiKey() const { return _multikey; }

        BSONObj currPK() const { return _currPK; }
        BSONObj currKey() const { return _currKey; }
        BSONObj indexKeyPattern() { return _idx.keyPattern(); }

        BSONObj current();
        string toString();

        BSONObj prettyKey( const BSONObj &key ) const {
            return key.replaceFieldNames( _idx.keyPattern() ).clientReadable();
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

        /** setup the PK and currKey fields based on the stored index key */
        void setKeyAndPK(const BSONObj &idxKey);
        /** setup DBC cursor and its initial position */
        void initializeDBC();
        /** check if the current key is out of bounds, invalidate the current key if so */
        bool checkCurrentAgainstBounds();
        bool skipOutOfRangeKeysAndCheckEnd();
        void checkEnd();

        // these are set in the construtor
        NamespaceDetails * const _d;

        // TODO: Get rid of _idxNo. It only exists because we need to know if the
        // _idxNo'th index is multikey using the NamespaceDetails. We should be
        // able to figure it out using the _idx;
        const int _idxNo;
        const IndexDetails& _idx;

        // these are all set in init()
        set<BSONObj> _dups;
        BSONObj _startKey;
        BSONObj _endKey;
        bool _endKeyInclusive;
        bool _multikey; // this must be updated every getmore batch in case someone added a multikey
        int _direction;
        shared_ptr< FieldRangeVector > _bounds;
        auto_ptr< FieldRangeVectorIterator > _boundsIterator;
        shared_ptr< CoveredIndexMatcher > _matcher;
        shared_ptr<Projection::KeyOnly> _keyFieldsOnly;
        bool _independentFieldRanges;
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
        Client::Transaction _transaction;
    };

} // namespace mongo;
