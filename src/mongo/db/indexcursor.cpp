// indexcursor.cpp

/**
*    Copyright (C) 2008 10gen Inc.
*    Copyright (C) 2013 Tokutek Inc.
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

    shared_ptr<IndexCursor> IndexCursor::make( NamespaceDetails *d, const IndexDetails &idx,
                                               const BSONObj &startKey, const BSONObj &endKey,
                                               bool endKeyInclusive, int direction,
                                               int numWanted ) {
        return shared_ptr<IndexCursor>( new IndexCursor( d, idx, startKey, endKey,
                                                         endKeyInclusive, direction,
                                                         numWanted ) );
    }

    shared_ptr<IndexCursor> IndexCursor::make( NamespaceDetails *d, const IndexDetails &idx,
                                               const shared_ptr< FieldRangeVector > &bounds,
                                               int singleIntervalLimit, int direction,
                                               int numWanted ) {
        return shared_ptr<IndexCursor>( new IndexCursor( d, idx, bounds,
                                                         singleIntervalLimit, direction,
                                                         numWanted ) );
    }

    IndexCursor::IndexCursor( NamespaceDetails *d, const IndexDetails &idx,
                              const BSONObj &startKey, const BSONObj &endKey,
                              bool endKeyInclusive, int direction, int numWanted ) :
        _d(d),
        _idx(idx),
        _ordering(Ordering::make(_idx.keyPattern())),
        _startKey(startKey),
        _endKey(endKey),
        _endKeyInclusive(endKeyInclusive),
        _multiKey(_d->isMultikey(_d->idxNo(_idx))),
        _direction(direction),
        _bounds(),
        _boundsIterator(),
        _nscanned(0),
        _nscannedObjects(0),
        _prelock(!cc().opSettings().getJustOne() && numWanted == 0),
        _cursor(_idx, cursor_flags()),
        _tailable(false),
        _ok(false),
        _getf_iteration(0),
        _bulkFetchWentOutBounds(false)
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
        _nscannedObjects(0),
        _prelock(!cc().opSettings().getJustOne() && numWanted == 0),
        _cursor(_idx, cursor_flags()),
        _tailable(false),
        _ok(false),
        _getf_iteration(0),
        _bulkFetchWentOutBounds(false)
    {
        verify( _d != NULL );
        _boundsIterator.reset( new FieldRangeVectorIterator( *_bounds , singleIntervalLimit ) );
        _boundsIterator->prepDive();
        _startKey = _bounds->startKey();
        _endKey = _bounds->endKey();
        _endKeyInclusive = _bounds->endKeyInclusive();
        TOKULOG(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        initializeDBC();

        // Fairly bad hack:
        //
        // Primary keys are not skipped properly when a non-inclusive start bound is specified.
        // See IndexCursor::skipToNextKey()
        //
        // Do a single advance here - the PK is unique so the next key is guaranteed to be
        // strictly greater than the start key.
        if (ok() && _d->isPKIndex(_idx) && !_bounds->startKeyInclusive() && _currKey == _startKey) {
            _advance();
        }
        DEV {
            // At this point, the current key should be consistent with
            // _startKey and _bounds->startKeyInclusive()
            if (ok() && !_bounds->startKeyInclusive()) {
                if (forward()) {
                    verify(_currKey.woCompare(_startKey, _ordering) > 0);
                } else {
                    verify(_currKey.woCompare(_startKey, _ordering) < 0);
                }
            }
        }
    }

    IndexCursor::~IndexCursor() {
        // Book-keeping for index access patterns.
        _idx.noteQuery(_nscanned, _nscannedObjects);
    }

    int IndexCursor::cursor_getf(const DBT *key, const DBT *val, void *extra) {
        struct cursor_getf_extra *info = static_cast<struct cursor_getf_extra *>(extra);
        try {
            // by default, state that we did not exceed our bounds
            info->cursor_went_out_bounds = false;
            if (key != NULL) {
                RowBuffer *buffer = info->buffer;
                storage::Key sKey(key);
                // only copy key if it matches the bounds
                info->buf_builder->reset();
                bool fetchRow = true;
                BSONObj currKey = sKey.key(*info->buf_builder);
                // if the key is no longer in bounds, don't fetch it
                if (info->end_checker && !info->end_checker->endInBounds(currKey)) {
                    fetchRow = false;
                    info->cursor_went_out_bounds = true;
                }
                info->num_scanned++;
                
                // if we have bounds and the key does not match the
                // bounds, don't fetch the row into the buffer
                if ( info->bounds && !info->bounds->matchesKey(currKey)) {
                    fetchRow = false;
                }
                
                if (fetchRow) {
                    buffer->append(sKey, val->size > 0 ?
                            BSONObj(static_cast<const char *>(val->data)) : BSONObj());
                    info->rows_fetched++;
                }

                // request more bulk fetching if we are allowed to fetch more rows
                // and the row buffer is not too full.
                if (!info->cursor_went_out_bounds &&
                    (info->rows_fetched < info->rows_to_fetch) &&
                    !buffer->isGorged()) {
                    return TOKUDB_CURSOR_CONTINUE;
                }
            }
            return 0;
        } catch (const std::exception &ex) {
            info->saveException(ex);
        }
        return -1;
    }

    void IndexCursor::setTailable() {
        // tailable cursors may not be created over secondary indexes,
        // and they must intend to read to the end of the collection.
        verify( _d->isPKIndex(_idx) );
        verify( _endKey.isEmpty() || _endKey == maxKey );
        // mark the cursor as tailable and set the end key bound tothe minimum unsafe
        // key to read from the namespace, non-inclusive.
        _tailable = true;
        _endKey = _d->minUnsafeKey();
        _endKeyInclusive = false;
        // Tailable cursors _must_ use endKey/endKeyInclusive so the bounds we
        // may or may not have gotten via the constructor is no longer valid.
        _bounds.reset();
        struct cursor_end_checker checker (&_endKey, _endKeyInclusive, _ordering, _direction);
        _ok = checker.endInBounds(_currKey);
    }

    bool IndexCursor::forward() const {
        return _direction > 0;
    }

    void IndexCursor::_prelockRange(const BSONObj &startKey, const BSONObj &endKey) {
        const bool isSecondary = !_d->isPKIndex(_idx);

        // The ydb requires that we only lock ranges such that the left
        // endpoint is less than or equal to the right endpoint.
        // Reverse cursors describe the start and end key as the two
        // keys where they start and end iteration, which is backwards
        // in the key space (because they iterate in reverse).
        const BSONObj &leftKey = forward() ? startKey : endKey; 
        const BSONObj &rightKey = forward() ? endKey : startKey; 
        dassert(leftKey.woCompare(rightKey, _ordering) <= 0);

        storage::Key sKey(leftKey, isSecondary ? &minKey : NULL);
        storage::Key eKey(rightKey, isSecondary ? &maxKey : NULL);
        DBT start = sKey.dbt();
        DBT end = eKey.dbt();

        DBC *cursor = _cursor.dbc();
        const int r = cursor->c_set_bounds( cursor, &start, &end, true, 0 );
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
            _prelockRange( startKey.done(), endKey.done() );
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

    void IndexCursor::_prelockBounds() {
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
                _prelockRange( startKey.done(), endKey.done() );
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

    // The ydb prelocking API serves two purposes: to enable prefetching
    // and acquire row locks. Row locks are acquired by serializable
    // transactions, serializable cursors, and RMW (write) cursors.
    //
    // If row locks are to be acquired, we _must_ prelock here, since we
    // pass DB_PRELOCKED | DB_PRELOCKED_WRITE to cursor operations.
    //
    // We would ideally use the ydb prelocking API to prelock each interval
    // as we iterated (via advance()), because there may be multiple intervals
    // and we can only lock/prefetch one at a time. Or, there could be separate
    // ydb APIs for prefetching and locking, which means we could take row locks
    // here if necessary and enable prefetching as we advance.
    //
    // Until that happens, we'll only enable prefetching if we think its worth it.
    // For simple start/end key cursors, it's always worth it, because it's
    // just one call to prelock. For bounds-based cursors, it is _probably_
    // worth it as long as the bounds vector isn't prefixed by a point
    // interval ($in, $or with equality). Most secondary indexes have
    // cardinality such that points (excluding appended PK) all fit in a
    // single basement node (64k of data, about), so prefetching wouldn't
    // have done anything. For non-points, we can't make any guess as to
    // how much data is in that range.
    void IndexCursor::prelock() {
        if (cc().txn().serializable() ||
            cc().opSettings().getQueryCursorMode() != DEFAULT_LOCK_CURSOR ||
            _bounds == NULL || !_bounds->prefixedByPointInterval()) {
            if ( _bounds != NULL ) {
                _prelockBounds();
            } else {
                _prelockRange( _startKey, _endKey );
            }
        }
    }

    void IndexCursor::initializeDBC() {
        if (_prelock) {
            // We need to prelock first, then position the cursor.
            prelock();
        }

        if ( _bounds != NULL ) {
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
            findKey( _startKey );
        }

        // at this point, we have positioned the cursor somewhere
        bool inRange = false;
        if (_bounds) {
            inRange = _bounds->matchesKey(_currKey);
        }
        else {
            struct cursor_end_checker checker (&_endKey, _endKeyInclusive, _ordering, _direction);
            inRange = checker.endInBounds(_currKey);
        }
        if (!inRange) {
            advance();
        }
    }

    int IndexCursor::cursor_flags() {
        QueryCursorMode mode = cc().opSettings().getQueryCursorMode();
        switch ( mode ) {
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

    int IndexCursor::getf_flags() {
        // Prelocked cursors do not need locks on getf().
        // Prelocked cursors should prefetch.
        const int lockFlags = _prelock ? (DB_PRELOCKED | DB_PRELOCKED_WRITE) : 0;
        const int prefetchFlags = _prelock ? 0 : DBC_DISABLE_PREFETCHING;
        return lockFlags | prefetchFlags;
    }

    int IndexCursor::getf_fetch_count() {
        bool shouldBulkFetch = cc().opSettings().shouldBulkFetch();
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
            return 1;
        }
    }

    void IndexCursor::findKey(const BSONObj &key) {
        const bool isSecondary = !_d->isPKIndex(_idx);
        const BSONObj &pk = forward() ? minKey : maxKey;
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
        shared_ptr<FieldRangeVector> emptyBounds;
        struct cursor_getf_extra extra(&_buffer, rows_to_fetch, emptyBounds, NULL, &_getfCallbackBufBuilder);
        DBC *cursor = _cursor.dbc();
        if ( forward() ) {
            r = cursor->c_getf_set_range(cursor, getf_flags(), &key_dbt, cursor_getf, &extra);
        } else {
            r = cursor->c_getf_set_range_reverse(cursor, getf_flags(), &key_dbt, cursor_getf, &extra);
        }
        if ( extra.ex != NULL ) {
            throw *extra.ex;
        }
        if ( r != 0 && r != DB_NOTFOUND ) {
            extra.throwException();
            storage::handle_ydb_error(r);
        }
        _nscanned += extra.num_scanned;

        _getf_iteration++;
        _ok = extra.rows_fetched > 0 ? true : false;
        if ( ok() ) {
            getCurrentFromBuffer();
        }

        TOKULOG(3) << "setPosition hit K, PK, Obj " << _currKey << _currPK << _currObj << endl;
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
                    forward() ? b.appendMinKey( "" ) : b.appendMaxKey( "" );
                } else {
                    // Regular ascending order. Max key skips forward.
                    forward() ? b.appendMaxKey( "" ) : b.appendMinKey( "" );
                }
            }
        }

        // This differs from findKey in that we set PK to max to move forward and min
        // to move backward, resulting in a "skip" of the key prefix, not a "find".
        const bool isSecondary = !_d->isPKIndex(_idx);
        const BSONObj &pk = forward() ? maxKey : minKey;
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
        // If after() is false, we use the same key prefix but set the remaining
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

    bool IndexCursor::fetchMoreRows() {
        // We're going to get more rows, so get rid of what's there.
        _buffer.empty();

        int r;
        const int rows_to_fetch = getf_fetch_count();
        struct cursor_end_checker end_checker(&_endKey, _endKeyInclusive, _ordering, _direction);
        struct cursor_getf_extra extra(&_buffer, rows_to_fetch, _bounds, &end_checker, &_getfCallbackBufBuilder);
        DBC *cursor = _cursor.dbc();
        if ( forward() ) {
            r = cursor->c_getf_next(cursor, getf_flags(), cursor_getf, &extra);
        } else {
            r = cursor->c_getf_prev(cursor, getf_flags(), cursor_getf, &extra);
        }
        _nscanned += extra.num_scanned;
        if (extra.cursor_went_out_bounds) {
            _bulkFetchWentOutBounds = true;
        }
        if ( extra.ex != NULL ) {
            throw *extra.ex;
        }
        if ( r != 0 && r != DB_NOTFOUND ) {
            extra.throwException();
            storage::handle_ydb_error(r);
        }
        else if (r == DB_NOTFOUND) {
            _bulkFetchWentOutBounds = true;
        }

        _getf_iteration++;
        return extra.rows_fetched > 0 ? true : false;
    }

    bool IndexCursor::advanceFromBuffer() {
        bool bufferAdvanced = _buffer.next();
        if (bufferAdvanced) {
            // we got a row, we are good
            getCurrentFromBuffer();
            TOKULOG(3) << "advance moved to K, PK, Obj" << _currKey << _currPK << _currObj << endl;
            _ok = true;
            return true;
        }
        return false;
    }

    bool IndexCursor::_advance() {
        if (!ok()) {
            return false;
        }
        
        // first check the RowBuffer. If something is there, we are good,
        // because we don't place it in the RowBuffer unless we KNOW it is
        // good to return
        // first try to get data from the bulk fetch buffer
        bool bufferAdvanced = advanceFromBuffer();
        if (bufferAdvanced) {
            return true;
        }

        // Now we are in the case where the RowBuffer has nothing,
        // but we should still be in bounds for the cursor. So, we need
        // to do fractal tree operations to refill the RowBuffer and get
        // rows to return
        while (ok()) {
            // if we went out of bounds, with our previous calls,
            // get out, there is nothing for this cursor to do.
            if (_bulkFetchWentOutBounds) {
                TOKULOG(3) << "advance exhausted, went past endKey" << endl;
                _ok = false;
                return false;
            }

            // get the last key read and see what the bounds iterator
            // says we should do
            if (_bounds) {
                // get the last key read from the cursor
                // ask the bounds iterator what the planned course
                // of action should be
                DBT lastKeyRead;
                memset(&lastKeyRead, 0, sizeof(lastKeyRead));
                lastKeyRead.flags = DB_DBT_REALLOC;                
                DBC *cursor = _cursor.dbc();
                int r = cursor->c_get_recent_key_read(cursor, &lastKeyRead);
                dassert(r == 0);                
                storage::Key sKey(&lastKeyRead);
                _getfCallbackBufBuilder.reset();                
                // now currKey is a BSONObj that represents the last
                // key this cursor has read
                BSONObj currKey = sKey.key(_getfCallbackBufBuilder);
                int skipIndex = skipToNextKey(currKey);
                if (skipIndex == -2) {
                    TOKULOG(3) << "skipIndex exhausted, went past endKey" << endl;
                    _bulkFetchWentOutBounds = true;
                    return false;
                }
                else if (skipIndex == -1) {
                    // continue with bulk fetching
                    bool gotMore = fetchMoreRows();
                    if (gotMore) {
                        // we got a row, we are good
                        getCurrentFromBuffer();
                        TOKULOG(3) << "advance moved to K, PK, Obj" << _currKey << _currPK << _currObj << endl;
                        _ok = true;
                        return true;
                    }
                    // otherwise, we go to the top of the loop and continue
                }
                else {
                    // the ugly case!
                    // we've skipped the key to another location
                    // check this new value to see if it is good
                    if (ok() && _bounds->matchesKey(_currKey)) {
                        // we are good! we have a row, get out
                        return true;
                    }
                    else {
                        // The row we retrieved does not match,
                        // keep going
                        continue;
                    }
                }
            }
            else {
                // we simply fetch some more
                // continue with bulk fetching
                bool gotMore = fetchMoreRows();
                if (gotMore) {
                    // we got a row, we are good
                    getCurrentFromBuffer();
                    TOKULOG(3) << "advance moved to K, PK, Obj" << _currKey << _currPK << _currObj << endl;
                    _ok = true;
                    return true;
                }
                // otherwise, we go to the top of the loop and continue
                // eventually, _bulkFetchWentOutBounds should become true
                // and we break out
            }
        }
        // if we get out here, we failed to advance, because ok() is false
        return false;
    }

    bool IndexCursor::advance() {
        killCurrentOp.checkForInterrupt();
        if ( ok() ) {
            // Advance one row further, and then check if we've went out of bounds.
            return _advance();
        } else {
            if ( tailable() ) {
                if ( _currKey < _endKey ) {
                    // Read the most up-to-date minUnsafeKey from the namespace
                    _endKey = _d->minUnsafeKey();
                    return _advance();
                } else {
                    // reset _currKey, we may have accidentally
                    // gone past _endKey when we did our last advance
                    // and saw something we are not allowed to see.
                    _currKey = _endKey;
                    // Read the most up-to-date minUnsafeKey from the namespace
                    _endKey = _d->minUnsafeKey();
                    findKey( _currKey.isEmpty() ? minKey : _currKey );
                    // tailable cursors don't have bounds anymore,
                    // so we can just return ok() here
                    return ok();
                }
            } else {
                // Exhausted cursors that are not tailable never advance
                return false;
            }
        }
    }

    BSONObj IndexCursor::current() {
        // If the index is clustering, the full documenet is always stored in _currObj.
        // If the index is not clustering, _currObj starts as empty and gets filled
        // with the full document on the first call to current().
        if ( _currObj.isEmpty() ) {
            _nscannedObjects++;
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
        bool shouldAppendPK = _d->isCapped() && cc().opSettings().shouldCappedAppendPK();
        if (shouldAppendPK) {
            BSONObjBuilder b;
            b.appendElements(_currObj);
            b.append("$_", _currPK);
            return b.obj();
        }
        return _currObj;
    }

    bool IndexCursor::currentMatches( MatchDetails *details ) {
         // Forward to the base class implementation, which may utilize a Matcher.
         return Cursor::currentMatches( details );
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

    ////////////////////////////////////////////////////////////////////////////////////////////////

    IndexCountCursor::IndexCountCursor( NamespaceDetails *d, const IndexDetails &idx,
                                        const BSONObj &startKey, const BSONObj &endKey,
                                        const bool endKeyInclusive ) :
        IndexCursor(d, idx, startKey, endKey, endKeyInclusive, 1, 0),
        _bufferedRowCount(0),
        _exhausted(false),
        _endSKeyPrefix(_endKey, NULL) {
        TOKULOG(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        checkAssumptionsAndInit();
    }

    IndexCountCursor::IndexCountCursor( NamespaceDetails *d, const IndexDetails &idx,
                                        const shared_ptr< FieldRangeVector > &bounds ) :
        // This will position the cursor correctly using bounds, which will do the right
        // thing based on bounds->start/endKey() and bounds->start/endKeyInclusive()
        IndexCursor(d, idx, bounds, false, 1, 0),
        _bufferedRowCount(0),
        _exhausted(false),
        _endSKeyPrefix(_endKey, NULL) {
        TOKULOG(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        dassert(_startKey == bounds->startKey());
        dassert(_endKey == bounds->endKey());
        dassert(_endKeyInclusive == bounds->endKeyInclusive());
        checkAssumptionsAndInit();
    }

    void IndexCountCursor::checkAssumptionsAndInit() {
        verify(forward()); // only need to count forward
        verify(_prelock); // should be no case where we decide not to prelock/prefetch
        if ( ok() ) {
            // ok() at this point means the IndexCursor constructor found
            // a single matching row. We note that with a buffered row
            // count of 1.
            _bufferedRowCount = 1;
        }
    }

    int IndexCountCursor::count_cursor_getf(const DBT *key, const DBT *val, void *extra) {
        struct count_cursor_getf_extra *info = reinterpret_cast<struct count_cursor_getf_extra *>(extra);
        try {
            // Initializes a storage key that will ignore the appended primary key.
            // This is useful because we only want to compare the secondary key prefix.
            const storage::Key sKey(reinterpret_cast<char *>(key->data), false);
            const int c = sKey.woCompare(info->endSKeyPrefix, info->ordering);
            TOKULOG(5) << "count getf got " << sKey.key() << ", c = " << c << endl;
            if (c > 0 || (c == 0 && !info->endKeyInclusive)) {
                // out of bounds, count is finished.
                dassert(!info->exhausted);
                info->exhausted = true;
                return 0;
            }
            info->bufferedRowCount++;
            return TOKUDB_CURSOR_CONTINUE;
        } catch (const std::exception &ex) {
            info->saveException(ex);
        }
        return -1;
    }

    bool IndexCountCursor::countMoreRows() {
        // no rows buffered for count, that's why we're counting more here.
        verify(_bufferedRowCount == 0);
        // should not have already tried + failed at counting more rows.
        verify(!_exhausted);

        DBC *cursor = _cursor.dbc();
        struct count_cursor_getf_extra extra(_bufferedRowCount, _exhausted,
                                             _endSKeyPrefix, _ordering, _endKeyInclusive);
        const int r = cursor->c_getf_next(cursor, getf_flags(), count_cursor_getf, &extra);
        if (r != 0 && r != DB_NOTFOUND) {
            extra.throwException();
            storage::handle_ydb_error(r);
        }
        return _bufferedRowCount > 0;
    }

    bool IndexCountCursor::advance() {
        killCurrentOp.checkForInterrupt();
        if ( ok() ) {
            dassert(_bufferedRowCount > 0);
            _ok = --_bufferedRowCount > 0;
            if ( !ok() && !_exhausted ) {
                _ok = countMoreRows();
            }
            if ( ok() ) {
                ++_nscanned;
            }
        }
        return ok();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    IndexScanCountCursor::IndexScanCountCursor( NamespaceDetails *d, const IndexDetails &idx ) :
        IndexCountCursor( d, idx,
                          ScanCursor::startKey(idx.keyPattern(), 1), 
                          ScanCursor::endKey(idx.keyPattern(), 1),
                          true ) {
        verify(forward());
    }

} // namespace mongo
