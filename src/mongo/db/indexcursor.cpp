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

    static int idx_cursor_flags() {
        QueryCursorMode mode = cc().tokuCommandSettings().getQueryCursorMode();
        switch ( mode ) {
            // All locks are grabbed up front, during initializeDBC().
            // These flags determine the type of lock. Serializable
            // gets you a read lock. Both serializable and rmw gets
            // you a write lock.
            case WRITE_LOCK_CURSOR:
                return DB_SERIALIZABLE | DB_RMW;
            case READ_LOCK_CURSOR:
                return DB_SERIALIZABLE;
            default:
                return 0;
        }
    }

    IndexCursor::IndexCursor( NamespaceDetails *d, const IndexDetails &idx,
                              const BSONObj &startKey, const BSONObj &endKey,
                              bool endKeyInclusive, int direction, int numWanted ) :
        _d(d),
        _idx(idx),
        _ordering(Ordering::make(_idx.keyPattern())),
        _startKey(_idx.getSpec().getType() ?
                _idx.getSpec().getType()->fixKey( startKey ) : startKey),
        _endKey(_idx.getSpec().getType() ?
                _idx.getSpec().getType()->fixKey( endKey ) : endKey),
        _endKeyInclusive(endKeyInclusive),
        _multiKey(_d->isMultikey(_d->idxNo(_idx))),
        _direction(direction),
        _bounds(),
        _nscanned(0),
        _numWanted(numWanted),
        _cursor(_idx, idx_cursor_flags()),
        _tailable(false),
        _ok(false),
        _getf_iteration(0)
    {
        verify( _d != NULL );
        TOKULOG(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        initializeDBC();
    }

    IndexCursor::IndexCursor( NamespaceDetails *d, const IndexDetails &idx,
                              const shared_ptr< FieldRangeVector > &bounds,
                              int singleIntervalLimit, int direction, int numWanted ) :
        _d(d),
        _idx(idx),
        _ordering(Ordering::make(_idx.keyPattern())),
        _startKey(),
        _endKey(),
        _endKeyInclusive(true),
        _multiKey(_d->isMultikey(_d->idxNo(_idx))),
        _direction(direction),
        _bounds(bounds),
        _nscanned(0),
        _numWanted(numWanted),
        _cursor(_idx, idx_cursor_flags()),
        _tailable(false),
        _ok(false),
        _getf_iteration(0)
    {
        verify( _d != NULL );
        _boundsIterator.reset( new FieldRangeVectorIterator( *_bounds , singleIntervalLimit ) );
        _boundsIterator->prepDive();
        _startKey = _bounds->startKey();
        _endKey = _bounds->endKey();
        TOKULOG(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        initializeDBC();
    }

    IndexCursor::~IndexCursor() {
    }

    void IndexCursor::setTailable() {
        // tailable cursors may not be created over secondary indexes, which means
        // this is a table scan cursor with trivial bounds.
        verify( _d->isPKIndex(_idx) );
        verify( _startKey.isEmpty() || _startKey == minKey );
        verify( _endKey.isEmpty() || _endKey == maxKey );
        // mark the cursor as tailable and set the end key bound tothe minimum unsafe
        // key to read from the namespace, non-inclusive.
        _tailable = true;
        _endKey = _d->minUnsafeKey();
        _endKeyInclusive = false;
        checkCurrentAgainstBounds();
    }

    void IndexCursor::prelockRange(const BSONObj &startKey, const BSONObj &endKey) {
        const bool isSecondary = !_d->isPKIndex(_idx);

        storage::Key sKey(startKey, isSecondary ? &minKey : NULL);
        storage::Key eKey(endKey, isSecondary ? &maxKey : NULL);
        DBT start = sKey.dbt();
        DBT end = eKey.dbt();

        DBC *cursor = _cursor.dbc();
        const int r = cursor->c_pre_acquire_range_lock( cursor, &start, &end );
        if ( r != 0 ) {
            storage::handle_ydb_error(r);
        }
    }

    void IndexCursor::_prelockCompoundBounds(const int currentRange,
                                             vector<const FieldInterval *> &combo,
                                             BufBuilder &startKeyBuilder,
                                             BufBuilder &endKeyBuilder) {
        const vector<FieldRange> &ranges = _bounds->ranges();
        if ( currentRange == (int) ranges.size() ) {
            startKeyBuilder.reset(512);
            endKeyBuilder.reset(512);
            BSONObjBuilder startKey(startKeyBuilder);
            BSONObjBuilder endKey(endKeyBuilder);

            for ( vector<const FieldInterval *>::const_iterator i = combo.begin();
                  i != combo.end(); i++ ) {
                startKey.appendAs( (*i)->_lower._bound, "" );
                endKey.appendAs( (*i)->_upper._bound, "" );
            }
            prelockRange( startKey.done(), endKey.done() );
        } else {
            const vector<FieldInterval> &intervals = ranges[currentRange].intervals();
            for ( vector<FieldInterval>::const_iterator i = intervals.begin();
                  i != intervals.end(); i++ ) {
                const FieldInterval &interval = *i;
                combo.push_back( &interval );
                _prelockCompoundBounds( currentRange + 1, combo, startKeyBuilder, endKeyBuilder );
                combo.pop_back();
            }
        }
    }

    void IndexCursor::prelockBounds() {
        BufBuilder startKeyBuilder(512);
        BufBuilder endKeyBuilder(512);

        const vector<FieldRange> &ranges = _bounds->ranges();
        const int n = ranges.size();
        dassert( n == _idx.keyPattern().nFields() );
        if ( n == 1 ) {
            // When there's only one field range, we can just prelock each interval.
            // Single field indexes are common so we handle this case manually for
            // performance (instead of using the recursive _prelockCompoundBounds())
            BSONObjBuilder startKey(startKeyBuilder);
            BSONObjBuilder endKey(endKeyBuilder);
            const vector<FieldInterval> &intervals = ranges[0].intervals();
            for ( vector<FieldInterval>::const_iterator i = intervals.begin();
                  i != intervals.end(); i++ ) {
                startKey.appendAs( i->_lower._bound, "" );
                endKey.appendAs( i->_upper._bound, "" );
                prelockRange( startKey.done(), endKey.done() );
            }
        } else {
            // When there's more than one field range, we need to prelock combinations
            // of intervals in the compound key space.
            verify( n > 1 );
            vector<const FieldInterval *> combo;
            combo.reserve( n );
            _prelockCompoundBounds( 0, combo, startKeyBuilder, endKeyBuilder );
        }
    }

    void IndexCursor::initializeDBC() {
        if ( _bounds != NULL ) {
            prelockBounds();
            const int r = skipToNextKey( _startKey );
            if ( r == -1 ) {
                // The bounds iterator suggests _bounds->startKey() is within
                // the current interval, so that's a good place to start. We
                // need to prepDive() on the iterator to reset its current
                // state so that further calls to skipToNextKey work properly.
                _boundsIterator->prepDive();
                findKey( _startKey );
            }
        } else {
            // Don't prelock point ranges.
            if ( _startKey != _endKey ) {
                prelockRange( _startKey, _endKey );
            }
            // Seek to an initial key described by _startKey 
            findKey( _startKey );
        }
        checkCurrentAgainstBounds();
    }

    int IndexCursor::getf_flags() {
        // Disable prefetching when a limit exists, to prevent unnecessary
        // IO and deserialization work. This will cause out-of-memory queries
        // with non-trivial limits to slow down, however. Not sure if that's bad.
        return _numWanted > 0 ? DBC_DISABLE_PREFETCHING : 0;
    }

    int IndexCursor::getf_fetch_count() {
        bool shouldBulkFetch = cc().tokuCommandSettings().shouldBulkFetch() && !tailable();
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
        const bool isSecondary = !_d->isPKIndex(_idx);
        const BSONObj &pk = _direction > 0 ? minKey : maxKey;
        setPosition(key, isSecondary ? pk : BSONObj());
    };

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
        if ( _direction > 0 ) {
            r = cursor->c_getf_set_range(cursor, getf_flags(), &key_dbt, cursor_getf, &extra);
        } else {
            r = cursor->c_getf_set_range_reverse(cursor, getf_flags(), &key_dbt, cursor_getf, &extra);
        }
        if ( r != 0 && r != DB_NOTFOUND ) {
            storage::handle_ydb_error(r);
        }

        _getf_iteration++;
        _ok = extra.rows_fetched > 0 ? true : false;
        if ( ok() ) {
            getCurrentFromBuffer();
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
        } else {
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
        const bool isSecondary = !_d->isPKIndex(_idx);
        const BSONObj &pk = _direction > 0 ? maxKey : minKey;
        setPosition( b.done(), isSecondary ? pk : BSONObj() );
    }

    int IndexCursor::skipToNextKey( const BSONObj &currentKey ) {
        int skipPrefixIndex = _boundsIterator->advance( currentKey );
        if ( skipPrefixIndex == -2 ) { 
            // We are done iterating completely.
            _ok = false;
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
        if ( i == 0 ) {
            return 0;
        } else {
            return i > 0 ? 1 : -1;
        }
    }

    // Check if the current key is beyond endKey.
    void IndexCursor::checkEnd() {
        if ( !ok() ) {
            return;
        }
        if ( !_endKey.isEmpty() ) {
            const int c = sgn( _endKey.woCompare( _currKey, _ordering ) );
            if ( (c != 0 && c != _direction) || (c == 0 && !_endKeyInclusive) ) {
                _ok = false;
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
        if ( _direction > 0 ) {
            r = cursor->c_getf_next(cursor, getf_flags(), cursor_getf, &extra);
        } else {
            r = cursor->c_getf_prev(cursor, getf_flags(), cursor_getf, &extra);
        }
        if ( r != 0 && r != DB_NOTFOUND ) {
            storage::handle_ydb_error(r);
        }

        _getf_iteration++;
        return extra.rows_fetched > 0 ? true : false;
    }

    void IndexCursor::_advance() {
        // first try to get data from the bulk fetch buffer
        _ok = _buffer.next();
        // if there is not data remaining in the bulk fetch buffer,
        // do a fractal tree call to get more rows
        if ( !ok() ) {
            _ok = fetchMoreRows();
        }
        // at this point, if there are rows to be gotten,
        // it is residing in the bulk fetch buffer.
        // Get a row from the bulk fetch buffer
        if ( ok() ) {
            getCurrentFromBuffer();
        }
        TOKULOG(3) << "_advance moved to K, PK, Obj" << _currKey << _currPK << _currObj << endl;
    }

    bool IndexCursor::advance() {
        killCurrentOp.checkForInterrupt();
        if ( ok() ) {
            // Advance one row further, and then check if we've went out of bounds.
            _advance();
        } else {
            if ( tailable() ) {
                if ( _currKey < _endKey ) {
                    _advance();
                } else {
                    // reset _currKey, we may have accidentally
                    // gone past _endKey when we did our last advance
                    // and saw something we are not allowed to see
                    _currKey = _endKey;
                    // The cursor advanced reached the minimum safe bound.
                    // Read a new safe bound from the namespace and reposition 
                    // to the current key. checkCurrent() will mark the cursor as ok()
                    // if the new current key is within bounds, and !ok() otherwise.
                    _endKey = _d->minUnsafeKey();
                    findKey( _currKey.isEmpty() ? minKey : _currKey );
                }
            } else {
                // Exhausted cursors that are not tailable never advance
                return false;
            }
        }
        // the key we are now positioned over may or may not be ok to read.
        // checkCurrentAgainstBounds() will decide.
        return checkCurrentAgainstBounds();
    }

    BSONObj IndexCursor::current() {
        // If the index is clustering, the full documenet is always stored in _currObj.
        // If the index is not clustering, _currObj starts as empty and gets filled
        // with the full document on the first call to current().
        if ( _currObj.isEmpty() ) {
            bool found = _d->findByPK( _currPK, _currObj );
            if ( !found ) {
                // If we didn't find the associated object, we must be either:
                // - a snapshot transaction whose context deleted the current pk
                // - a read uncommitted cursor with stale data
                // In either case, we may advance and try again exactly once.
                TOKULOG(4) << "current() did not find associated object for pk " << _currPK << endl;
                advance();
                if ( ok() ) {
                    found = _d->findByPK( _currPK, _currObj );
                    uassert( 16741, str::stream()
                                << toString() << ": could not find associated document with pk "
                                << _currPK << ", index key " << _currKey, found );
                }
            }
        }
        bool shouldAppendPK = _d->isCapped() && cc().tokuCommandSettings().shouldCappedAppendPK();
        if (shouldAppendPK) {
            BSONObjBuilder b;
            b.appendElements(_currObj);
            b.append("$_", _currPK);
            return b.obj();
        }
        return _currObj;
    }

    string IndexCursor::toString() const {
        string s = string("IndexCursor ") + _idx.indexName();
        if ( _direction < 0 ) {
            s += " reverse";
        }
        if ( _bounds.get() && _bounds->size() > 1 ) {
            s += " multi";
        }
        return s;
    }
    
    BSONObj IndexCursor::prettyIndexBounds() const {
        if ( _bounds == NULL ) {
            return BSON( "start" << prettyKey( _startKey ) << "end" << prettyKey( _endKey ) );
        } else {
            return _bounds->obj();
        }
    }    

} // namespace mongo
