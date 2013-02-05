// btreecursor.cpp

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

#include "pch.h"
#include "jsobj.h"
#include "curop-inl.h"
#include "queryutil.h"

#include "mongo/db/namespace_details.h"
#include "mongo/db/toku/cursor.h"

namespace mongo {

    BtreeCursor* BtreeCursor::make(
        NamespaceDetails *_d, const IndexDetails& _id,
        const shared_ptr< FieldRangeVector > &_bounds, int _direction )
    {
        return make( _d, _d->idxNo( (IndexDetails&) _id), _id, _bounds, 0, _direction );
    }

    BtreeCursor* BtreeCursor::make(
        NamespaceDetails *_d, const IndexDetails& _id,
        const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction)
    {
        return make( _d, _d->idxNo( (IndexDetails&) _id), _id, startKey, endKey, endKeyInclusive, direction );
    }


    BtreeCursor* BtreeCursor::make( NamespaceDetails * nsd , int idxNo , const IndexDetails& indexDetails ) {
        //int v = indexDetails.version();
        
        //if( v == 2 ) 
            return new TokuDBCursor( nsd , idxNo , indexDetails );

#if 0
        if( v == 1 ) 
            return new BtreeCursorImpl<V1>( nsd , idxNo , indexDetails );
        
        if( v == 0 ) 
            return new BtreeCursorImpl<V0>( nsd , idxNo , indexDetails );

        dassert( IndexDetails::isASupportedIndexVersionNumber(v) );
        uasserted(14800, str::stream() << "unsupported index version " << v);
        return 0; // not reachable
#endif
    }

    // TODO: Get rid of all but one of these static constructors
    
    BtreeCursor* BtreeCursor::make(
        NamespaceDetails *d, int idxNo, const IndexDetails& id, 
        const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction) 
    { 
        auto_ptr<BtreeCursor> c( make( d , idxNo , id ) );
        c->init(startKey,endKey,endKeyInclusive,direction);
        c->initWithoutIndependentFieldRanges();
        dassert( c->_dups.size() == 0 );
        return c.release();
    }

    BtreeCursor* BtreeCursor::make(
        NamespaceDetails *d, int idxNo, const IndexDetails& id, 
        const shared_ptr< FieldRangeVector > &bounds, int singleIntervalLimit, int direction )
    {
        auto_ptr<BtreeCursor> c( make( d , idxNo , id ) );
        c->init(bounds,singleIntervalLimit,direction);
        return c.release();
    }

    BtreeCursor::BtreeCursor( NamespaceDetails* nsd , int theIndexNo, const IndexDetails& id ) 
        : d( nsd ) , idxNo( theIndexNo ) , indexDetails( id ) , _ordering(Ordering::make(BSONObj())){
        _nscanned = 0;
    }

    void BtreeCursor::_finishConstructorInit() {
        _multikey = d->isMultikey( idxNo );
        _order = indexDetails.keyPattern();
        _ordering = Ordering::make( _order );
    }
    
    void BtreeCursor::init( const BSONObj& sk, const BSONObj& ek, bool endKeyInclusive, int direction ) {
        _finishConstructorInit();
        startKey = sk;
        endKey = ek;
        _endKeyInclusive =  endKeyInclusive;
        _direction = direction;
        _independentFieldRanges = false;
        audit();
    }

    void BtreeCursor::init(  const shared_ptr< FieldRangeVector > &bounds, int singleIntervalLimit, int direction ) {
        _finishConstructorInit();
        _bounds = bounds;
        verify( _bounds );
        _direction = direction;
        _endKeyInclusive = true;
        _boundsIterator.reset( new FieldRangeVectorIterator( *_bounds , singleIntervalLimit ) );
        _independentFieldRanges = true;
        audit();
        startKey = _bounds->startKey();
        _boundsIterator->advance( startKey ); // handles initialization
        _boundsIterator->prepDive();
        // TODO: TokuDB: Get rid of the bucket member.
        //bucket = indexDetails.head;
        ::abort();
    }

    /** Properly destroy forward declared class members. */
    BtreeCursor::~BtreeCursor() {}
    
    void BtreeCursor::audit() {
        dassert( d->idxNo((IndexDetails&) indexDetails) == idxNo );
    }

    void BtreeCursor::initWithoutIndependentFieldRanges() {
        if ( indexDetails.getSpec().getType() ) {
            startKey = indexDetails.getSpec().getType()->fixKey( startKey );
            endKey = indexDetails.getSpec().getType()->fixKey( endKey );
        }
        //bucket = _locate(startKey, _direction > 0 ? minDiskLoc : maxDiskLoc);
        ::abort();
        if ( ok() ) {
            _nscanned = 1;
        }
        checkEnd();
    }

    void BtreeCursor::skipAndCheck() {
        long long startNscanned = _nscanned;
        while( 1 ) {
            if ( !skipOutOfRangeKeysAndCheckEnd() ) {
                break;
            }
            do {
                if ( _nscanned > startNscanned + 20 ) {
                    return;
                }
            } while( skipOutOfRangeKeysAndCheckEnd() );
            break;
        }
    }

    // XXX: TokuDB: I think boundsIterator iterates over keybounds like [0,2] [100,200] so you would
    // need to reposition the cursor each time you hit a new range. that's what skip out of range
    // keys and check end probably means.
    bool BtreeCursor::skipOutOfRangeKeysAndCheckEnd() {
        if ( !ok() ) {
            return false;
        }
        int ret = _boundsIterator->advance( currKey() );
        if ( ret == -2 ) {
            //bucket = DiskLoc();
            ::abort();
            return false;
        }
        else if ( ret == -1 ) {
            ++_nscanned;
            return false;
        }
        ++_nscanned;
        advanceTo( currKey(), ret, _boundsIterator->after(), _boundsIterator->cmp(), _boundsIterator->inc() );
        return true;
    }

    // Return a value in the set {-1, 0, 1} to represent the sign of parameter i.
    int sgn( int i ) {
        if ( i == 0 )
            return 0;
        return i > 0 ? 1 : -1;
    }

    // Check if the current key is beyond endKey.
    void BtreeCursor::checkEnd() {
#if 0
        if ( bucket.isNull() )
            return;
        if ( !endKey.isEmpty() ) {
            int cmp = sgn( endKey.woCompare( currKey(), _order ) );
            if ( ( cmp != 0 && cmp != _direction ) ||
                    ( cmp == 0 && !_endKeyInclusive ) )
                bucket = DiskLoc();
        }
#endif
        ::abort();
    }

    void BtreeCursor::advanceTo( const BSONObj &keyBegin, int keyBeginLen, bool afterKey, const vector< const BSONElement * > &keyEnd, const vector< bool > &keyEndInclusive) {
        // XXX: TokuDB: What do we do here?
        ::abort();
#if 0
        int keyOfs = 0;
        _advanceTo( bucket, keyOfs, keyBegin, keyBeginLen, afterKey, keyEnd, keyEndInclusive, _ordering, _direction );
#endif
    }

    bool BtreeCursor::advance() {
        killCurrentOp.checkForInterrupt();
        // XXX: TokuDB: What do we do here?
#if 0
        if ( bucket.isNull() )
            return false;
        
        int keyOfs = 0;
        bucket = _advance(bucket, keyOfs, _direction, "BtreeCursor::advance");
#endif
        
        if ( !_independentFieldRanges ) {
            checkEnd();
            if ( ok() ) {
                ++_nscanned;
            }
        }
        else {
            skipAndCheck();
        }
        return ok();
    }

    string BtreeCursor::toString() {
        string s = string("BtreeCursor ") + indexDetails.indexName();
        if ( _direction < 0 ) s += " reverse";
        if ( _bounds.get() && _bounds->size() > 1 ) s += " multi";
        return s;
    }
    
    BSONObj BtreeCursor::prettyIndexBounds() const {
        if ( !_independentFieldRanges ) {
            return BSON( "start" << prettyKey( startKey ) << "end" << prettyKey( endKey ) );
        }
        else {
            return _bounds->obj();
        }
    }    

    /* ----------------------------------------------------------------------------- */

    struct BtreeCursorUnitTest {
        BtreeCursorUnitTest() {
            //verify( minDiskLoc.compare(maxDiskLoc) < 0 );
        }
    } btut;

} // namespace mongo
