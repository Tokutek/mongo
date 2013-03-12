// indexcursor.cpp

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
#include "mongo/db/storage/key.h"

namespace mongo {

    RowBuffer::RowBuffer() :
        _size(_BUF_SIZE_PREFERRED),
        _current_offset(0),
        _end_offset(0),
        _buf(new char[_size]) {
    }

    RowBuffer::~RowBuffer() {
        delete []_buf;
    }

    // append the given key, pk and obj to the end of the buffer
    void RowBuffer::append(const BSONObj &key, const BSONObj &pk, const BSONObj &obj) {
        size_t key_size = key.objsize();
        size_t pk_size = pk.objsize();
        size_t obj_size = obj.objsize();
        size_t size_needed = _end_offset + key_size + pk_size + obj_size;

        // if we need more than we have, realloc.
        if (size_needed > _size) {
            char *buf = new char[size_needed];
            memcpy(buf, _buf, _size);
            delete []_buf;
            _buf = buf;
            _size = size_needed;
        }

        // append the key, update the end offset
        memcpy(_buf + _end_offset, key.objdata(), key_size);
        _end_offset += key_size;
        // append the pk, update the end offset
        memcpy(_buf + _end_offset, pk.objdata(), pk_size);
        _end_offset += pk_size;
        // append the obj, update the end offset
        memcpy(_buf + _end_offset, obj.objdata(), obj_size);
        _end_offset += obj_size;

        // postcondition: end offset is correctly bounded
        verify(_end_offset <= _size);
    }

    // move the internal buffer position to the next key, pk, obj triple
    // returns:
    //      true, the buffer is reading to be read via current()
    //      false, the buffer has no more data. don't call next again without append()'ing.
    bool RowBuffer::next() {

        // if the buffer has more, seek passed the current one
        if (ok()) {
            size_t key_size = currentKey().objsize();
            size_t pk_size = currentPK().objsize();
            size_t obj_size = currentObj().objsize();
            _current_offset += key_size + pk_size + obj_size;
        }

        // postcondition: we did not seek passed the end of the buffer.
        verify(_current_offset <= _end_offset);

        return ok();
    }

    // empty the row buffer, resetting all data and internal positions
    // only reset it fields if there is something in the buffer.
    void RowBuffer::empty() {
        if ( _end_offset > 0 ) {
            // If the row buffer got really big, bring it back down to size.
            // Otherwise it's okay if its within 2x preferred size.
            if ( _size > _BUF_SIZE_PREFERRED * 2 ) {
                delete []_buf;
                _size = _BUF_SIZE_PREFERRED;
                _buf = new char[_size];
            }
            _current_offset = 0;
            _end_offset = 0;
        }
    }

    /* ---------------------------------------------------------------------- */

    struct cursor_getf_extra {
        RowBuffer *buffer;
        int rows_fetched;
        int rows_to_fetch;
        cursor_getf_extra(RowBuffer *buf, int n_to_fetch) :
            buffer(buf), rows_fetched(0), rows_to_fetch(n_to_fetch) {
        }
    };

    // ydb layer cursor callback
    static int cursor_getf(const DBT *key, const DBT *val, void *extra) {
        int r = 0;

        // the cursor callback is called even if the desired
        // key is not found. in that case, key == NULL
        if (key) {
            struct cursor_getf_extra *info = static_cast<struct cursor_getf_extra *>(extra);
            RowBuffer *buffer = info->buffer;

            BSONObj keyObj, pkObj, valObj;

            // There is always a non-empty bson object key to start.
            keyObj = BSONObj(static_cast<char *>(key->data));
            verify(keyObj.objsize() <= (int) key->size);
            verify(!keyObj.isEmpty());

            // Check if there a PK attached to the end of the first key.
            // If not, then this is the primary index, so PK == key.
            if (keyObj.objsize() < (int) key->size) {
                pkObj = BSONObj(static_cast<char *>(key->data) + keyObj.objsize());
                verify(keyObj.objsize() + pkObj.objsize() == (int) key->size);
                verify(!pkObj.isEmpty());
            } else {
                pkObj = keyObj;
            }

            // Check if an object lives in the val buffer.
            if (val->size > 0) {
                valObj = BSONObj(static_cast<char *>(val->data));
                verify(valObj.objsize() == (int) val->size);
            } else {
                valObj = BSONObj();
            }

            // Append the new row to the buffer.
            buffer->append(keyObj, pkObj, valObj);
            tokulog(3) << "cursor_getf appended to row buffer " << keyObj << pkObj << valObj << endl;
            
            // request more bulk fetching if we are allowed to fetch more rows
            // and the row buffer is not too full.
            if (++info->rows_fetched < info->rows_to_fetch && !buffer->isGorged()) {
                r = TOKUDB_CURSOR_CONTINUE;
            }
        }

        return r;
    }

    IndexCursor::IndexCursor( NamespaceDetails *d, const IndexDetails *idx,
            const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction, int numWanted ) :
        _d(d),
        _idx(idx),
        _ordering(Ordering::make(_idx != NULL ? _idx->keyPattern() : BSONObj())),
        _startKey(_idx != NULL && _idx->getSpec().getType() ?
                   _idx->getSpec().getType()->fixKey( startKey ) : startKey),
        _endKey(_idx != NULL && _idx->getSpec().getType() ?
                 _idx->getSpec().getType()->fixKey( endKey ) : endKey),
        _endKeyInclusive(endKeyInclusive),
        _multiKey(_d != NULL && _idx != NULL ? _d->isMultikey(_d->idxNo(*_idx)) : false),
        _direction(direction),
        _bounds(),
        _nscanned(0),
        _numWanted(numWanted),
        _cursor(NULL),
        _tailable(false),
        _readOnly(cc().getContext()->isReadOnly()),
        _getf_iteration(0)
    {
        tokulog(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        initializeDBC();
    }

    IndexCursor::IndexCursor( NamespaceDetails *d, const IndexDetails *idx,
            const shared_ptr< FieldRangeVector > &bounds, int singleIntervalLimit, int direction, int numWanted ) :
        _d(d),
        _idx(idx),
        _ordering(Ordering::make(_idx != NULL ? _idx->keyPattern() : BSONObj())),
        _startKey(),
        _endKey(),
        _endKeyInclusive(true),
        _multiKey(_d != NULL && _idx != NULL ? _d->isMultikey(_d->idxNo(*_idx)) : false),
        _direction(direction),
        _bounds(bounds),
        _nscanned(0),
        _numWanted(numWanted),
        _cursor(NULL),
        _tailable(false),
        _readOnly(cc().getContext()->isReadOnly()),
        _getf_iteration(0)
    {
        tokulog(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        _boundsIterator.reset( new FieldRangeVectorIterator( *_bounds , singleIntervalLimit ) );
        _boundsIterator->prepDive();
        initializeDBC();
    }

    IndexCursor::~IndexCursor() {
        if (_cursor != NULL) {
            const int r = _cursor->c_close(_cursor);
            verify(r == 0);
        }
    }

    void IndexCursor::prelockRange(const BSONObj &startKey, const BSONObj &endKey) {
        const bool isSecondary = !_idx->isIdIndex();

        storage::Key sKey(startKey, isSecondary ? &minKey : NULL);
        storage::Key eKey(endKey, isSecondary ? &maxKey : NULL);
        DBT start = sKey.dbt();
        DBT end = eKey.dbt();

        const int r = _cursor->c_pre_acquire_range_lock( _cursor, &start, &end );
        if ( r != 0 ) {
            StringBuilder s;
            s << toString() << ": failed to acquire prelocked range on " <<
                prettyIndexBounds() << ", ydb error " << r << ". Try again.";
            uasserted( 16447, s.str() );
        }
                
    }

    void IndexCursor::initializeDBC() {
        // _d and _idx are mutually null when the collection doesn't
        // exist and is therefore treated as empty.
        if (_d != NULL && _idx != NULL) {
            _cursor = _idx->newCursor();
            if ( _bounds != NULL) {
                // Try skipping forward in the key space using the bounds iterator
                // and the proposed startKey. If skipping wasn't necessary, then
                // use that start key to set our position and reset the iterator.
                prelockRange( _bounds->startKey(), _bounds->endKey() );
                const int r = skipToNextKey( _bounds->startKey() );
                if ( r == -1 ) {
                    // The bounds iterator suggests _bounds->startKey() is within
                    // the current interval, so that's a good place to start. We
                    // need to prepDive() on the iterator to reset its current
                    // state so that further calls to skipToNextKey work properly.
                    _boundsIterator->prepDive();
                    findKey( _bounds->startKey() );
                }
            } else {
                // Seek to an initial key described by _startKey 
                prelockRange( _startKey, _endKey );
                findKey( _startKey );
            }
            checkCurrentAgainstBounds();
        } else {
            verify( _d == NULL && _idx == NULL );
        }
    }

    int IndexCursor::getf_flags() {
        // Read-only cursors pass no special flags, non read-only cursors pass
        // DB_RMW in order to obtain write locks in the ydb-layer.
        const int lockFlags = _readOnly ? 0 : DB_RMW;
        const int prefetchFlags = _numWanted > 0 ? DBC_DISABLE_PREFETCHING : 0;
        return lockFlags | prefetchFlags;
    }

    int IndexCursor::getf_fetch_count() {
        if ( _readOnly ) {
            // Read-only cursor may bulk fetch rows into a buffer, for speed.
            // The number of rows fetched is proportional to the number of
            // times we've called getf.
            switch ( _getf_iteration ) {
                case 0:
                    return 1;
                case 1:
                    return 16;
                case 2:
                    return 128;
                case 3:
                    return 1024;
                case 4:
                    return 4096;
                default:
                    return 16384;
            }
        } else {
            // Cursors that are not read only may not buffer rows, because they
            // may perform a write some time in the future and possibly invalidate
            // buffered data.
            return 1;
        }
    }

    void IndexCursor::findKey(const BSONObj &key) {
        const bool isSecondary = !_idx->isIdIndex();
        const BSONObj &pk = _direction > 0 ? minKey : maxKey;
        setPosition(key, isSecondary ? pk : BSONObj());
    };

    void IndexCursor::setPosition(const BSONObj &key, const BSONObj &pk) {
        tokulog(3) << toString() << ": setPosition(): getf " << key << ", pk " << pk << ", direction " << _direction << endl;

        // Empty row buffer, reset fetch iteration, go get more rows.
        _buffer.empty();
        _getf_iteration = 0;

        storage::Key sKey( key, !pk.isEmpty() ? &pk : NULL );
        DBT key_dbt = sKey.dbt();;

        int r;
        const int rows_to_fetch = getf_fetch_count();
        struct cursor_getf_extra extra(&_buffer, rows_to_fetch);
        if (_direction > 0) {
            r = _cursor->c_getf_set_range(_cursor, getf_flags(), &key_dbt, cursor_getf, &extra);
        } else {
            r = _cursor->c_getf_set_range_reverse(_cursor, getf_flags(), &key_dbt, cursor_getf, &extra);
        }
        verify(r == 0 || r == DB_NOTFOUND);
        _getf_iteration++;
        _currKey = extra.rows_fetched > 0 ? _buffer.currentKey() : BSONObj();
        _currPK = extra.rows_fetched > 0 ? _buffer.currentPK() : BSONObj();
        _currObj = extra.rows_fetched > 0 ? _buffer.currentObj() : BSONObj();
        tokulog(3) << "setPosition hit K, PK, Obj " << _currKey << _currPK << _currObj << endl;
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

    // Skip the key comprised of the first k fields of currentKey and the
    // rest set to max/min key for direction > 0 or < 0 respectively.
    void IndexCursor::skipPrefix(const BSONObj &key, const int k) {
        tokulog(3) << "skipPrefix skipping first " << k << " elements in key " << key << endl;
        BSONObjBuilder b;
        BSONObjIterator it = key.begin();
        for ( int i = 0; i < key.nFields(); i++ ) {
            if ( i < k ) {
                b.append( it.next() );
            } else {
                if ( _ordering.descending( 1 << i ) ) {
                    // Descending sort order, so min key skips forward.
                    _direction > 0 ? b.appendMinKey( "" ) : b.appendMaxKey( "" );
                } else {
                    // Regular ascending order. Max key skips forward.
                    _direction > 0 ? b.appendMaxKey( "" ) : b.appendMinKey( "" );
                }
            }
        }

        // This differs from findKey in that we set PK to max to move forward and min
        // to move backward, resulting in a "skip" of the key prefix, not a "find".
        const bool isSecondary = !_idx->isIdIndex();
        const BSONObj &pk = _direction > 0 ? maxKey : minKey;
        setPosition( b.done(), isSecondary ? pk : BSONObj() );
    }

    int IndexCursor::skipToNextKey( const BSONObj &currentKey ) {
        int skipPrefixIndex = _boundsIterator->advance( currentKey );
        if ( skipPrefixIndex == -2 ) { 
            // We are done iterating completely.
            _currKey = BSONObj();
            return -2;
        }
        else if ( skipPrefixIndex == -1 ) { 
            // We should skip nothing.
            return -1;
        }
    
        // We should skip to a further key, efficiently.
        //
        // If after(), skip to the first key greater/less than the key comprised
        // of the first "skipPrefixIndex" elements of currentKey, and the rest
        // set to MaxKey/MinKey for direction > 0 and direction < 0 respectively.
        // eg: skipPrefixIndex = 1, currKey {a:1, b:2, c:1}, direction > 0,  so we skip
        // to the first key greater than {a:1, b:maxkey, c:maxkey}
        //
        // If after() is false, we use the same key prefix but set the reamining
        // elements to the elements described by cmp(), in order.
        // eg: skipPrefixIndex = 1, currKey {a:1, b:2, c:1}) and cmp() [b:5, c:11]
        // so we use skip to {a:1, b:5, c:11}, also noting direction.
        if ( _boundsIterator->after() ) {
            skipPrefix( currentKey, skipPrefixIndex );
        } else {
            BSONObjBuilder b;
            BSONObjIterator it = currentKey.begin();
            const vector<const BSONElement *> endKeys = _boundsIterator->cmp();
            for ( int i = 0; i < currentKey.nFields(); i++ ) {
                if ( i < skipPrefixIndex ) {
                    verify( it.more() );
                    b.append( it.next() );
                } else {
                    b.appendAs( *endKeys[i] , "" );
                }
            }
            findKey( b.done() );

            // Skip passed key prefixes that are not supposed to be inclusive
            // as described by _boundsIterator->inc() and endKeys
            //
            // We'll spend at worst nFields^2 time ensuring all key elements
            // are properly set if all the inclusive bits are false and we
            // keep landing on keys where the ith element of curr == endkeys[i].
            //
            // This complexity is usually ok, since this skipping is supposed to
            // save us from really big linear scans across the key space in
            // some pathological cases. It's not clear whether or not small
            // cases are hurt too badly by this algorithm.
            while ( ok() ) {
                BSONObj key = currKey();
                it = key.begin();
                const vector<bool> &inclusive = _boundsIterator->inc();
                for ( int i = 0; i < key.nFields(); i++ ) {
                    const BSONElement e = it.next();
                    if ( i >= skipPrefixIndex && !inclusive[i] && e.valuesEqual(*endKeys[i]) ) {
                        // The ith element equals the ith endKey but it's not supposed to be inclusive.
                        // Skipping to the next value for the ith element involves skipping a prefix 
                        // with i + 1 elements.
                        skipPrefix( key, i + 1 );
                        continue;
                    }
                }
                break;
            }
        }
        return 0;
    }

    bool IndexCursor::skipOutOfRangeKeysAndCheckEnd() {
        if ( ok() ) { 
            // If r is -2, the cursor is exhausted. We're not supposed to count that.
            const int r = skipToNextKey( currKey() );
            if ( r != -2 ) {
                _nscanned++;
            }
            return r == 0;
        }
        return false;
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
            const int cmp = sgn( _endKey.woCompare( currKey(), _idx->keyPattern() ) );
            if ( ( cmp != 0 && cmp != _direction ) ||
                    ( cmp == 0 && !_endKeyInclusive ) ) {
                _currKey = BSONObj();
                tokulog(3) << toString() << ": checkEnd() stopping @ curr, end: " << currKey() << _endKey << endl;
            }
        }
    }

    bool IndexCursor::fetchMoreRows() {
        // We're going to get more rows, so get rid of what's there.
        _buffer.empty();

        int r;
        const int rows_to_fetch = getf_fetch_count();
        struct cursor_getf_extra extra(&_buffer, rows_to_fetch);
        if (_direction > 0) {
            r = _cursor->c_getf_next(_cursor, getf_flags(), cursor_getf, &extra);
        } else {
            r = _cursor->c_getf_prev(_cursor, getf_flags(), cursor_getf, &extra);
        }
        _getf_iteration++;
        verify(r == 0 || r == DB_NOTFOUND);
        return extra.rows_fetched > 0 ? true : false;
    }

    void IndexCursor::_advance() {
        // namespace might be null if we're tailing an empty collection
        if ( _d != NULL && _idx != NULL ) {
            bool ok = _buffer.next();
            if ( !ok ) {
                ok = fetchMoreRows();
            }
            _currKey = ok ? _buffer.currentKey() : BSONObj();
            _currPK = ok ? _buffer.currentPK() : BSONObj();
            _currObj = ok ? _buffer.currentObj() : BSONObj();
            tokulog(3) << "_advance moved to K, PK, Obj" << _currKey << _currPK << _currObj << endl;
        } else {
            // new inserts will not be read by this cursor, because there was no
            // namespace details or index at the time of creation. we can either
            // accept this caveat or try to fix it. at least emit a warning.
            if ( tailable() ) {
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
        if ( _currObj.isEmpty() && _d != NULL ) {
            verify( _idx != NULL );
            bool found = _d->findById( _currPK, _currObj, false );
            if ( !found ) {
                // If we didn't find the associated object, we must be a non read-only
                // cursor whose context deleted the current _id. In this case, we are
                // allowed to advance and try again exactly once. If we still can't
                // find the object, we're in trouble.
                verify( !_readOnly );
                tokulog(4) << "current() did not find associated object for pk " << _currPK << endl;
                advance();
                if ( ok() ) {
                    found = _d->findById( _currPK, _currObj, false );
                    verify( found );
                }
            }
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

} // namespace mongo
