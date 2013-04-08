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

    bool RowBuffer::ok() const {
        return _current_offset < _end_offset;
    }

    bool RowBuffer::isGorged() const {
        const int threshold = 100;
        const bool almost_full = _end_offset + threshold > _size;
        const bool too_big = _size > _BUF_SIZE_PREFERRED;
        return almost_full || too_big;
    }

    // get the current key/pk/obj from the buffer, or set them
    // to empty if they don't exist.
    void RowBuffer::current(storage::Key &sKey, BSONObj &obj) const {
        dassert(ok());

        const char *buf = _buf + _current_offset;
        const char headerBits = *buf++;
        dassert(headerBits >= 1 && headerBits <= 3);

        storage::Key sk(buf, headerBits & HeaderBits::hasPK);
        sKey.set(buf, sk.size());
        obj = headerBits & HeaderBits::hasObj ? BSONObj(buf + sKey.size()) : BSONObj();

        dassert(_current_offset
                + 1
                + sk.size()
                + (headerBits & HeaderBits::hasObj ? obj.objsize() : 0)
                <= _end_offset);
    }

    void RowBuffer::append(const storage::Key &sKey, const BSONObj &obj) {

        size_t key_size = sKey.size();
        size_t obj_size = obj.isEmpty() ? 0 : obj.objsize();
        size_t size_needed = _end_offset + 1 + key_size + obj_size;

        // if we need more than we have, realloc.
        if (size_needed > _size) {
            char *buf = new char[size_needed];
            memcpy(buf, _buf, _size);
            delete []_buf;
            _buf = buf;
            _size = size_needed;
        }

        // Determine what to put in the header byte.
        const bool hasPK = !sKey.pk().isEmpty();
        const bool hasObj = obj_size > 0;
        const unsigned char headerBits = (hasPK ? HeaderBits::hasPK : 0) | (hasObj ? HeaderBits::hasObj : 0);
        dassert(headerBits >= 1 && headerBits <= 3);
        memcpy(_buf + _end_offset, &headerBits, 1);
        _end_offset += 1;

        // Append the new key/obj row to the buffer.
        // We'll know how to interpet it later because
        // the header bit says whether a pk/obj exists.
        memcpy(_buf + _end_offset, sKey.buf(), key_size);
        _end_offset += key_size;
        if (obj_size > 0) {
            memcpy(_buf + _end_offset, obj.objdata(), obj_size);
            _end_offset += obj_size;
        }

        verify(_end_offset <= _size);
    }

    // moves the internal position to the next key/pk/obj and returns them
    // returns:
    //      true, the buffer had more and key/pk/obj were set appropriately
    //      false, the buffer has no more data. don't call current() until append()
    bool RowBuffer::next() {
        if (!ok()) {
            return false;
        }

        // the buffer has more, seek passed the current one.
        const char headerBits = *(_buf + _current_offset);
        dassert(headerBits >= 1 && headerBits <= 3);
        _current_offset += 1;

        storage::Key sKey(_buf + _current_offset, headerBits & HeaderBits::hasPK);
        _current_offset += sKey.size();

        if (headerBits & HeaderBits::hasObj) {
            BSONObj obj(_buf + _current_offset);
            _current_offset += obj.objsize();
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
            storage::Key sKey(key);
            buffer->append(sKey, val->size > 0 ?
                    BSONObj(static_cast<const char *>(val->data)) : BSONObj());

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
        _cursor(_idx),
        _tailable(false),
        _getf_iteration(0)
    {
        TOKULOG(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
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
        _cursor(_idx),
        _tailable(false),
        _getf_iteration(0)
    {
        TOKULOG(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        _boundsIterator.reset( new FieldRangeVectorIterator( *_bounds , singleIntervalLimit ) );
        _boundsIterator->prepDive();
        initializeDBC();
    }

    IndexCursor::~IndexCursor() {
    }

    void IndexCursor::prelockRange(const BSONObj &startKey, const BSONObj &endKey) {
        const bool isSecondary = !_d->isPKIndex(*_idx);

        storage::Key sKey(startKey, isSecondary ? &minKey : NULL);
        storage::Key eKey(endKey, isSecondary ? &maxKey : NULL);
        DBT start = sKey.dbt();
        DBT end = eKey.dbt();

        DBC *cursor = _cursor.dbc();
        const int r = cursor->c_pre_acquire_range_lock( cursor, &start, &end );
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
            // Don't prelock point ranges.
            if ( _bounds != NULL) {
                // Try skipping forward in the key space using the bounds iterator
                // and the proposed startKey. If skipping wasn't necessary, then
                // use that start key to set our position and reset the iterator.
                const BSONObj startKey = _bounds->startKey();
                const BSONObj endKey = _bounds->endKey();
                // Don't prelock ranges when there's a field range vector until we
                // know how to isolate the start/end keys for the current interval.
                if ( false && startKey != endKey ) {
                    prelockRange( startKey, endKey );
                }
                const int r = skipToNextKey( startKey );
                if ( r == -1 ) {
                    // The bounds iterator suggests _bounds->startKey() is within
                    // the current interval, so that's a good place to start. We
                    // need to prepDive() on the iterator to reset its current
                    // state so that further calls to skipToNextKey work properly.
                    _boundsIterator->prepDive();
                    findKey( startKey );
                }
            } else {
                // Seek to an initial key described by _startKey 
                if ( _startKey != _endKey ) {
                    prelockRange( _startKey, _endKey );
                }
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
        int lockFlags = 0;
        QueryCursorMode lockMode = cc().tokuCommandSettings().getQueryCursorMode();
        switch (lockMode) {
            case READ_LOCK_CURSOR:
                lockFlags |= DB_SERIALIZABLE;
                break;
            case WRITE_LOCK_CURSOR:
                lockFlags |= (DB_RMW | DB_SERIALIZABLE);
                break;
            case DEFAULT_LOCK_CURSOR:
                break;
        }
        const int prefetchFlags = _numWanted > 0 ? DBC_DISABLE_PREFETCHING : 0;
        return lockFlags | prefetchFlags;
    }

    int IndexCursor::getf_fetch_count() {
        bool shouldBulkFetch = cc().tokuCommandSettings().shouldBulkFetch();
        if ( shouldBulkFetch ) {
            // Read-only cursor may bulk fetch rows into a buffer, for speed.
            // The number of rows fetched is proportional to the number of
            // times we've called getf.
            switch ( _getf_iteration ) {
                case 0:
                case 1:
                    // The first and second iterations should only fetch
                    // 1 row, to optimize point queries.
                    return 1;
                default:
                    return 2 << (_getf_iteration < 20 ? _getf_iteration : 20);
            }
        } else {
            // Cursors that are not read only may not buffer rows, because they
            // may perform a write some time in the future and possibly invalidate
            // buffered data.
            return 1;
        }
    }

    void IndexCursor::findKey(const BSONObj &key) {
        const bool isSecondary = !_d->isPKIndex(*_idx);
        const BSONObj &pk = _direction > 0 ? minKey : maxKey;
        setPosition(key, isSecondary ? pk : BSONObj());
    };

    void IndexCursor::exhausted() {
        _currKey = BSONObj();
        _currPK = BSONObj();
        _currObj = BSONObj();
    }

    void IndexCursor::getCurrentFromBuffer() {
        storage::Key sKey;
        _buffer.current(sKey, _currObj);

        _currKeyBufBuilder.reset(512);
        _currKey = sKey.key(_currKeyBufBuilder);
        _currPK = sKey.pk();
        if (_currPK.isEmpty()) {
            _currPK = BSONObj(_currKey.objdata());
        }
    }

    void IndexCursor::setPosition(const BSONObj &key, const BSONObj &pk) {
        TOKULOG(3) << toString() << ": setPosition(): getf " << key << ", pk " << pk << ", direction " << _direction << endl;

        // Empty row buffer, reset fetch iteration, go get more rows.
        _buffer.empty();
        _getf_iteration = 0;

        storage::Key sKey( key, !pk.isEmpty() ? &pk : NULL );
        DBT key_dbt = sKey.dbt();;

        int r;
        const int rows_to_fetch = getf_fetch_count();
        struct cursor_getf_extra extra(&_buffer, rows_to_fetch);
        DBC *cursor = _cursor.dbc();
        if (_direction > 0) {
            r = cursor->c_getf_set_range(cursor, getf_flags(), &key_dbt, cursor_getf, &extra);
        } else {
            r = cursor->c_getf_set_range_reverse(cursor, getf_flags(), &key_dbt, cursor_getf, &extra);
        }
        verify(r == 0 || r == DB_NOTFOUND || r == DB_LOCK_NOTGRANTED || r == DB_LOCK_DEADLOCK);
        uassert(ASSERT_ID_LOCK_NOTGRANTED, "tokudb lock not granted", r != DB_LOCK_NOTGRANTED);
        uassert(ASSERT_ID_LOCK_DEADLOCK, "tokudb deadlock", r != DB_LOCK_DEADLOCK);
        _getf_iteration++;
        if (extra.rows_fetched > 0) {
            getCurrentFromBuffer();
        } else {
            exhausted();
        }
        TOKULOG(3) << "setPosition hit K, PK, Obj " << _currKey << _currPK << _currObj << endl;
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
                } while ( skipOutOfRangeKeysAndCheckEnd() );
            }
        }
        return ok();
    }

    // Skip the key comprised of the first k fields of currentKey and the
    // rest set to max/min key for direction > 0 or < 0 respectively.
    void IndexCursor::skipPrefix(const BSONObj &key, const int k) {
        TOKULOG(3) << "skipPrefix skipping first " << k << " elements in key " << key << endl;
        BSONObjBuilder b(key.objsize());
        BSONObjIterator it = key.begin();
        const int nFields = key.nFields();
        for ( int i = 0; i < nFields; i++ ) {
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
        const bool isSecondary = !_d->isPKIndex(*_idx);
        const BSONObj &pk = _direction > 0 ? maxKey : minKey;
        setPosition( b.done(), isSecondary ? pk : BSONObj() );
    }

    int IndexCursor::skipToNextKey( const BSONObj &currentKey ) {
        int skipPrefixIndex = _boundsIterator->advance( currentKey );
        if ( skipPrefixIndex == -2 ) { 
            // We are done iterating completely.
            exhausted();
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
            BSONObjBuilder b(currentKey.objsize());
            BSONObjIterator it = currentKey.begin();
            const vector<const BSONElement *> &endKeys = _boundsIterator->cmp();
            const int nFields = currentKey.nFields();
            for ( int i = 0; i < nFields; i++ ) {
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
            bool allInclusive = true;
            const vector<bool> &inclusive = _boundsIterator->inc();
            for ( int i = 0; i < nFields; i++ ) {
                if ( !inclusive[i] ) {
                    allInclusive = false;
                    break;
                }
            }
again:      while ( !allInclusive && ok() ) {
                BSONObj key = _currKey;
                it = key.begin();
                dassert( nFields == key.nFields() );
                for ( int i = 0; i < nFields; i++ ) {
                    const BSONElement e = it.next();
                    if ( i >= skipPrefixIndex && !inclusive[i] && e.valuesEqual(*endKeys[i]) ) {
                        // The ith element equals the ith endKey but it's not supposed to be inclusive.
                        // Skipping to the next value for the ith element involves skipping a prefix 
                        // with i + 1 elements.
                        skipPrefix( key, i + 1 );
                        goto again;
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
            const int r = skipToNextKey( _currKey );
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
        if ( _currKey.isEmpty() ) {
            return;
        }
        if ( !_endKey.isEmpty() ) {
            dassert( _d != NULL &&_idx != NULL );
            // TODO: Change _idx->keyPattern() to _ordering, which is cheaper
            //const int cmp = sgn( _endKey.woCompare( _currKey, _idx->keyPattern() ) );
            const int cmp = sgn( _endKey.woCompare( _currKey, _ordering ) );
            if ( ( cmp != 0 && cmp != _direction ) ||
                    ( cmp == 0 && !_endKeyInclusive ) ) {
                exhausted()
                TOKULOG(3) << toString() << ": checkEnd() stopping @ curr, end: " << _currKey << _endKey << endl;
            }
        }
    }

    bool IndexCursor::fetchMoreRows() {
        // We're going to get more rows, so get rid of what's there.
        _buffer.empty();

        int r;
        const int rows_to_fetch = getf_fetch_count();
        struct cursor_getf_extra extra(&_buffer, rows_to_fetch);
        DBC *cursor = _cursor.dbc();
        if (_direction > 0) {
            r = cursor->c_getf_next(cursor, getf_flags(), cursor_getf, &extra);
        } else {
            r = cursor->c_getf_prev(cursor, getf_flags(), cursor_getf, &extra);
        }
        _getf_iteration++;
        verify(r == 0 || r == DB_NOTFOUND || r == DB_LOCK_NOTGRANTED || r == DB_LOCK_DEADLOCK);
        uassert(ASSERT_ID_LOCK_NOTGRANTED, "tokudb lock not granted", r != DB_LOCK_NOTGRANTED);
        uassert(ASSERT_ID_LOCK_DEADLOCK, "tokudb deadlock", r != DB_LOCK_DEADLOCK);
        return extra.rows_fetched > 0 ? true : false;
    }

    void IndexCursor::_advance() {
        // namespace might be null if we're tailing an empty collection.
        if ( _d != NULL && _idx != NULL ) {
            bool ok = _buffer.next();
            if ( !ok ) {
                ok = fetchMoreRows();
            }
            if ( ok ) {
                getCurrentFromBuffer();
            } else {
                exhausted();
            }
            TOKULOG(3) << "_advance moved to K, PK, Obj" << _currKey << _currPK << _currObj << endl;
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
            bool found = _d->findByPK( _currPK, _currObj );
            if ( !found ) {
                // If we didn't find the associated object, we must be a non read-only
                // cursor whose context deleted the current pk. In this case, we are
                // allowed to advance and try again exactly once. If we still can't
                // find the object, we're in trouble.
                //verify( !_readOnly );
                // TODO: (John), find a better invariant than above
                TOKULOG(4) << "current() did not find associated object for pk " << _currPK << endl;
                advance();
                if ( ok() ) {
                    found = _d->findByPK( _currPK, _currObj );
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
