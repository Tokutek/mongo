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

#include "mongo/pch.h"
#include "mongo/db/curop.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/cursor.h"

// TODO: dassert isn't working for some reason, so we call verify instead.
//       this might be slow.

namespace mongo {

    struct cursor_getf_extra {
        BSONObj *const key;
        BSONObj *const pk;
        BSONObj *const val;
        cursor_getf_extra(BSONObj *const k, BSONObj *const p, BSONObj *const v)
            : key(k), pk(p), val(v) { }
    };

    static int cursor_getf(const DBT *key, const DBT *val, void *extra) {
        struct cursor_getf_extra *info = static_cast<struct cursor_getf_extra *>(extra);
        verify(key != NULL);
        verify(val != NULL);

        // There is always a non-empty bson object key to start.
        BSONObj keyObj(static_cast<char *>(key->data));
        verify(keyObj.objsize() <= (int) key->size);
        verify(!keyObj.isEmpty());
        *info->key = keyObj.getOwned();

        // Check if there a PK attached to the end of the first key.
        // If not, then this is the primary index, so PK == key.
        if (keyObj.objsize() < (int) key->size) {
            BSONObj pkObj(static_cast<char *>(key->data) + keyObj.objsize());
            verify(keyObj.objsize() + pkObj.objsize() == (int) key->size);
            verify(!pkObj.isEmpty());
            *info->pk = pkObj.getOwned();
        } else {
            *info->pk = *info->key;
        }

        // Check if an object lives in the val buffer.
        if (val->size > 0) {
            BSONObj valObj(static_cast<char *>(val->data));
            verify(valObj.objsize() == (int) val->size);
            *info->val = valObj.isEmpty() ? BSONObj() : valObj.getOwned();
        }
        return 0;
    }

    IndexCursor::IndexCursor( NamespaceDetails *d, const IndexDetails *idx,
            const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction ) :
        _d(d),
        _idx(idx),
        _startKey(_idx != NULL && _idx->getSpec().getType() ?
                   _idx->getSpec().getType()->fixKey( startKey ) : startKey),
        _endKey(_idx != NULL && _idx->getSpec().getType() ?
                 _idx->getSpec().getType()->fixKey( endKey ) : endKey),
        _endKeyInclusive(endKeyInclusive),
        _multiKey(_d != NULL && _idx != NULL ? _d->isMultikey(_d->idxNo(*_idx)) : false),
        _direction(direction),
        _bounds(),
        _nscanned(0),
        _cursor(NULL),
        _tailable(false)
    {
        tokulog(1) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        initializeDBC();
    }

    IndexCursor::IndexCursor( NamespaceDetails *d, const IndexDetails *idx,
            const shared_ptr< FieldRangeVector > &bounds, int singleIntervalLimit, int direction ) :
        _d(d),
        _idx(idx),
        _startKey(),
        _endKey(),
        _endKeyInclusive(true),
        _multiKey(_d != NULL && _idx != NULL ? _d->isMultikey(_d->idxNo(*_idx)) : false),
        _direction(direction),
        _bounds(bounds),
        _nscanned(0),
        _cursor(NULL),
        _tailable(false)
    {
        _boundsIterator.reset( new FieldRangeVectorIterator( *_bounds , singleIntervalLimit ) );
        _startKey = _bounds->startKey();
        _boundsIterator->advance( _startKey ); // handles initialization
        _boundsIterator->prepDive();
        tokulog(1) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        initializeDBC();
    }

    IndexCursor::~IndexCursor() {
        if (_cursor != NULL) {
            int r = _cursor->c_close(_cursor);
            verify(r == 0);
        }
    }

    void IndexCursor::initializeDBC() {
        // _d and _idx are mutually null when the collection doesn't
        // exist and is therefore treated as empty.
        if (_d != NULL && _idx != NULL) {
            _cursor = _idx->cursor();
            setPosition(_startKey);
            checkCurrentAgainstBounds();
        } else {
            verify( _d == NULL && _idx == NULL );
        }
    }

    void IndexCursor::setPosition(const BSONObj &key) {
        // Reset keys and objects
        _currKey = BSONObj();
        _currPK = BSONObj();
        _currObj = BSONObj();

        int r;
        struct cursor_getf_extra extra(&_currKey, &_currPK, &_currObj);
        DBT key_dbt;
        key_dbt.data = const_cast<char *>(key.objdata());
        key_dbt.size = key.objsize();
        tokulog(1) << toString() << ": setPosition(): getf key " << key << ", direction " << _direction << endl;
        if (_direction > 0) {
            r = _cursor->c_getf_set_range(_cursor, 0, &key_dbt, cursor_getf, &extra);
        } else {
            r = _cursor->c_getf_set_range_reverse(_cursor, 0, &key_dbt, cursor_getf, &extra);
        }
        verify(r == 0 || r == DB_NOTFOUND);
        tokulog(1) << toString() << ": setPosition(): hit K, PK, Obj " << _currKey << _currPK << _currObj << endl;
    }

    // Check the current key with respect to our key bounds, whether
    // it be provided by independent field ranges or by start/end keys.
    bool IndexCursor::checkCurrentAgainstBounds() {
        if ( _bounds == NULL ) {
            checkEnd();
            if ( ok() ) {
                ++_nscanned;
            }
        }
        else {
            long long startNscanned = _nscanned;
            if ( skipOutOfRangeKeysAndCheckEnd() ) {
                do {
                    if ( _nscanned > startNscanned + 20 ) {
                        break;
                    }
                } while( skipOutOfRangeKeysAndCheckEnd() );
            }
        }
        return ok();
    }

    bool IndexCursor::skipOutOfRangeKeysAndCheckEnd() {
        BSONObj currentKey = currKey();
        if ( !ok() ) { 
            return false;
        }
        int skipPrefix = _boundsIterator->advance( currentKey );
        if ( skipPrefix == -2 ) { 
            // We are done iterating completely.
            _currKey = BSONObj();
            return false;
        }
        else if ( skipPrefix == -1 ) { 
            // We should skip nothing.
            ++_nscanned;
            return false;
        }
    
        // We should skip to a further key, efficiently.
        //
        // If after(), skip to the first key greater/less than the key comprised
        // of the first "skipPrefix" elements of currentKey, and the rest
        // set to MaxKey/MinKey for direction > 0 and direction < 0 respectively.
        // eg: skipPrefix = 1, currKey {a:1, b:2, c:1}, direction > 0,  so we skip
        // to the first key greater than {a:1, b:maxkey, c:maxkey}
        //
        // If after() is false, we use the same key prefix but set the reamining
        // elements to the elements described by cmp(), in order.
        // eg: skipPrefix = 1, currKey {a:1, b:2, c:1}) and cmp() [b:5, c:11]
        // so we use skip to {a:1, b:5, c:11}, also noting direction.
        BSONObjBuilder b;
        BSONObjIterator it = currentKey.begin();
        for ( int i = 0; i < skipPrefix; i++ ) {
            verify( it.more() );
            b.append( it.next() );
        }
        if ( _boundsIterator->after() ) {
            // Fill out the rest of the fields with nameless maxKey elements
            for ( int i = 0; i < currentKey.nFields() - skipPrefix; i++) {
                _direction > 0 ? b.appendMaxKey( "" ) : b.appendMinKey( "" );
            }
            setPosition( b.done() );
        } else {
            // Keep our own copy of endKeys that we can modify.
            const vector<const BSONElement *> endKeys = _boundsIterator->cmp();
            const vector<bool> &inclusive = _boundsIterator->inc();
            for ( int i = skipPrefix; i < currentKey.nFields(); i++ ) {
                b.append( *endKeys[i] );
            }
            setPosition( b.done() );

            // Skip passed any keys that have elements after the skipPrefix
            // where the ith element equals the respective endkey, but it
            // was not supposed to be inclusive.
            currentKey = currKey();
            it = currentKey.begin();
            for ( int i = 0; i < currentKey.nFields(); i++ ) {
                const BSONElement e = it.next();
                if ( i > skipPrefix && !inclusive[i] && e == *endKeys[i] ) {
                    tokulog(1) << "skipOutOfRangeKeys skipping currKey " << currentKey << " because "
                        " the element at index " << i << " is non-inclusive, " << e << endl;
                    _advance();
                    continue;
                }
            }
        }
        ++_nscanned;
        return true;
    }

    // Return a value in the set {-1, 0, 1} to represent the sign of parameter i.
    int sgn( int i ) {
        if ( i == 0 )
            return 0;
        return i > 0 ? 1 : -1;
    }

    // Check if the current key is beyond endKey.
    void IndexCursor::checkEnd() {
        if ( _currKey.isEmpty() )
            return;
        if ( !_endKey.isEmpty() ) {
            verify( _idx != NULL );
            int cmp = sgn( _endKey.woCompare( currKey(), _idx->keyPattern() ) );
            if ( ( cmp != 0 && cmp != _direction ) ||
                    ( cmp == 0 && !_endKeyInclusive ) ) {
                _currKey = BSONObj();
                tokulog(1) << toString() << ": checkEnd() stopping @ curr, end: " << currKey() << _endKey << endl;
            }
        }
    }

    void IndexCursor::_advance() {
        // namespace might be null if we're tailing an empty collection
        if ( _d != NULL && _idx != NULL ) {
            // Reset current key/pk/obj to empty.
            _currKey = BSONObj();
            _currPK = BSONObj();
            _currObj = BSONObj();

            int r;
            struct cursor_getf_extra extra(&_currKey, &_currPK, &_currObj);
            if (_direction > 0) {
                r = _cursor->c_getf_next(_cursor, 0, cursor_getf, &extra);
            } else {
                r = _cursor->c_getf_prev(_cursor, 0, cursor_getf, &extra);
            }
            verify(r == 0 || r == DB_NOTFOUND);
            tokulog(2) << toString() << ": _advance() moved to K, P, Obj " << _currKey << _currPK << _currObj << endl;
        } else {
            // new inserts will not be read by this cursor, because there was no
            // namespace details or index at the time of creation. we can either
            // accept this caveat or try to fix it. at least emit a warning.
            if (tailable()) {
                problem() 
                    << "Attempted to advance a tailable cursor on an empty collection! " << endl
                    << "The current implementation cannot read new writes from any cursor " << endl
                    << "created when the collection was empty. Try again with a new cursor " << endl
                    << "when the collection is non-empty." << endl;
            }
        }
    }

    bool IndexCursor::advance() {
        killCurrentOp.checkForInterrupt();
        if ( _currKey.isEmpty() && !tailable() ) {
            return false;
        } else {
            _advance();
            return checkCurrentAgainstBounds();
        }
    }

    BSONObj IndexCursor::current() {
        // If the index is clustering, the full documenet is always stored in _currObj.
        // If the index is not clustering, _currObj starts as empty and gets filled
        // with the full document on the first call to current().
        if (_currObj.isEmpty() && _d != NULL) {
            verify(_idx != NULL);
            verify(!_currKey.isEmpty());
            verify(!_currPK.isEmpty());
            tokulog(1) << toString() << ": current() _currKey: " << _currKey << ", PK " << _currPK << endl;
            bool found = _d->findById(_currPK, _currObj, false);
            tokulog(1) << toString() << ": current() PK lookup res: " << _currObj << endl;
            verify(found);
            verify(!_currObj.isEmpty());
        }
        return _currObj;
    }

    string IndexCursor::toString() const {
        string s = string("IndexCursor ") + (_idx != NULL ? _idx->indexName() : "(null)");
        if ( _direction < 0 ) s += " reverse";
        if ( _bounds.get() && _bounds->size() > 1 ) s += " multi";
        return s;
    }
    
    BSONObj IndexCursor::prettyIndexBounds() const {
        if ( _bounds == NULL ) {
            return BSON( "start" << prettyKey( _startKey ) << "end" << prettyKey( _endKey ) );
        }
        else {
            return _bounds->obj();
        }
    }    

    /* ----------------------------------------------------------------------------- */

    struct IndexCursorUnitTest {
        IndexCursorUnitTest() {
            //verify( minDiskLoc.compare(maxDiskLoc) < 0 );
        }
    } btut;

} // namespace mongo
