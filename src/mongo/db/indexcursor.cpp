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
        verify( ok() );

        // seek passed the current key, loc, and obj.
        size_t key_size = currentKey().objsize();
        size_t pk_size = currentPK().objsize();
        size_t obj_size = currentObj().objsize();
        _current_offset += key_size + pk_size + obj_size;

        // postcondition: we did not seek passed the end of the buffer.
        verify(_current_offset <= _end_offset);

        return ok();
    }

    // empty the row buffer, resetting all data and internal positions
    // only reset it fields if there is something in the buffer.
    void RowBuffer::empty() {
        if ( _end_offset > 0 ) {
            delete []_buf;
            _size = _BUF_SIZE_PREFERRED;
            _buf = new char[_size];
            _current_offset = 0;
            _end_offset = 0;
        }
    }

    /* ------------------------------------------------------------------ */

    // how many rows should we fetch for a particular iteration?
    // crude performance testing shows us that it is quite expensive 
    // to go into go into ydb layer and start bulk fetching. we should
    // fetch exponentially more and more rows with each iteration so
    // that we avoid too much ydb layer overhead without reading it
    // all at once, which is too aggressive for queries that might
    // not want all of it.
    static int max_rows_to_fetch(int iteration) {
        int rows_to_fetch = 1;
        switch (iteration) {
            case 0:
                rows_to_fetch = 1;
                break;
            case 1:
                rows_to_fetch = 16;
                break;
            case 2:
                rows_to_fetch = 128;
                break;
            case 3:
                rows_to_fetch = 1024;
                break;
            case 4:
                rows_to_fetch = 4096;
                break;
            default:
                rows_to_fetch = 16384;
                break;
        }
        return rows_to_fetch;
    }

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
            tokulog(1) << "cursor_getf appended to row buffer " << keyObj << pkObj << valObj << endl;
            
            // request more bulk fetching if we are allowed to fetch more rows
            // and the row buffer is not too full.
            if (++info->rows_fetched < info->rows_to_fetch && !buffer->isGorged()) {
                r = TOKUDB_CURSOR_CONTINUE;
            }
        }

        return r;
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
        _tailable(false),
        _getf_iteration(0)
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
        _tailable(false),
        _getf_iteration(0)
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
        DBT key_dbt;
        key_dbt.data = const_cast<char *>(key.objdata());
        key_dbt.size = key.objsize();
        tokulog(1) << toString() << ": setPosition(): getf key " << key << ", direction " << _direction << endl;

        // Empty row buffer, reset fetch iteration, go get more rows.
        _buffer.empty();
        _getf_iteration = 0;

        int r;
        int rows_to_fetch = max_rows_to_fetch(_getf_iteration);
        struct cursor_getf_extra extra(&_buffer, rows_to_fetch);
        if (_direction > 0) {
            r = _cursor->c_getf_set_range(_cursor, 0, &key_dbt, cursor_getf, &extra);
        } else {
            r = _cursor->c_getf_set_range_reverse(_cursor, 0, &key_dbt, cursor_getf, &extra);
        }
        _getf_iteration++;
        _currKey = extra.rows_fetched > 0 ? _buffer.currentKey() : BSONObj();
        _currPK = extra.rows_fetched > 0 ? _buffer.currentPK() : BSONObj();
        _currObj = extra.rows_fetched > 0 ? _buffer.currentObj() : BSONObj();
        tokulog(1) << "setPosition hit K, P, Obj " << _currKey << _currPK << _currObj << endl;
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
    void IndexCursor::skipPrefix(int k) {
        tokulog(1) << "skipPrefix skipping first " << k << " elements in key " << endl;
        BSONObjBuilder b;
        BSONObjIterator it = _currKey.begin();
        for ( int i = 0; i < _currKey.nFields(); i++ ) {
            if ( i < k ) {
                b.append( it.next() );
            } else {
                _direction > 0 ? b.appendMaxKey( "" ) : b.appendMinKey( "" );
            }
        }
        setPosition( b.done() );
    }

    bool IndexCursor::skipOutOfRangeKeysAndCheckEnd() {
        BSONObj currentKey = currKey();
        if ( !ok() ) { 
            return false;
        }
        int skipPrefixIndex = _boundsIterator->advance( currentKey );
        if ( skipPrefixIndex == -2 ) { 
            // We are done iterating completely.
            _currKey = BSONObj();
            return false;
        }
        else if ( skipPrefixIndex == -1 ) { 
            // We should skip nothing.
            ++_nscanned;
            return false;
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
            skipPrefix(skipPrefixIndex);
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
            tokulog(1) << "skipOutOfRngeKeys used first " << skipPrefixIndex << " elements"
                " in key and the rest from cmp(), setting position now..." << endl;
            setPosition( b.done() );

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
                currentKey = currKey();
                it = currentKey.begin();
                const vector<bool> &inclusive = _boundsIterator->inc();
                for ( int i = 0; i < currentKey.nFields(); i++ ) {
                    const BSONElement e = it.next();
                    if ( i > skipPrefixIndex && !inclusive[i] && e == *endKeys[i] ) {
                        tokulog(1) << "skipOutOfRangeKeys skipping currKey " << currentKey << " because "
                            " the element at index " << i << " is non-inclusive, " << e << endl;
                        // The ith element equals the ith endKey but it's not supposed to be inclusive.
                        skipPrefix( i );
                        continue;
                    }
                }
                break;
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

    bool IndexCursor::fetchMoreRows() {
        // We're going to get more rows, so get rid of what's there.
        _buffer.empty();

        int r;
        int rows_to_fetch = max_rows_to_fetch(_getf_iteration);
        struct cursor_getf_extra extra(&_buffer, rows_to_fetch);
        if (_direction > 0) {
            r = _cursor->c_getf_next(_cursor, 0, cursor_getf, &extra);
        } else {
            r = _cursor->c_getf_prev(_cursor, 0, cursor_getf, &extra);
        }
        _getf_iteration++;
        verify(r == 0 || r == DB_NOTFOUND);
        tokulog(1) << "fetchMoreRows reached iteration " << _getf_iteration <<
            " by fetching " << extra.rows_fetched << " rows" << endl;
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
            tokulog(1) << "_advance moved to K, PK, Obj" << _currKey << _currPK << _currObj << endl;
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
        if ( _currObj.isEmpty() && _d != NULL ) {
            verify(_idx != NULL);
            tokulog(1) << toString() << ": current() _currKey: " << _currKey << ", PK " << _currPK << endl;
            bool found = _d->findById(_currPK, _currObj, false);
            tokulog(1) << toString() << ": current() PK lookup res: " << _currObj << endl;
            verify(found);
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
