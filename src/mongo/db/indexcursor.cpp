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
#include "mongo/db/cursor.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/collection.h"

namespace mongo {

    RowBuffer::RowBuffer() :
        _size(1024),
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
            // grow our size aggressively, and at least to size_needed bytes
            size_t new_size = std::max(size_needed, 4 * _size);
            char *buf = new char[new_size];
            memcpy(buf, _buf, _size);
            delete []_buf;
            _buf = buf;
            _size = new_size;
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

    IndexCursor::IndexCursor( CollectionData *cl, const IndexDetails &idx,
                              const BSONObj &startKey, const BSONObj &endKey,
                              bool endKeyInclusive, int direction, int numWanted ) :
        _cl(cl),
        _idx(idx),
        _ordering(Ordering::make(_idx.keyPattern())),
        // Geo indexes (2d) require that we massage the start/end key before using them
        _startKey(idx.fixKey(startKey)),
        _endKey(idx.fixKey(endKey)),
        _endKeyInclusive(endKeyInclusive),
        _multiKey(cl->isMultiKey(cl->idxNo(idx))),
        _direction(direction),
        _bounds(),
        _boundsMustMatch(true),
        _nscanned(0),
        _nscannedObjects(0),
        _prelock(!cc().opSettings().getJustOne() && numWanted == 0),
        _tailable(false),
        _ok(false),
        _getf_iteration(0)
    {
        verify( _cl != NULL );
        TOKULOG(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        _cursor = idx.getCursor(cursor_flags());
        DBC* cursor = _cursor->dbc();
        cursor->c_set_check_interrupt_callback(cursor, cursor_check_interrupt, &_interrupt_extra);
        initializeDBC();
    }

    IndexCursor::IndexCursor( CollectionData *cl, const IndexDetails &idx,
                              const shared_ptr< FieldRangeVector > &bounds,
                              int singleIntervalLimit, int direction, int numWanted ) :
        _cl(cl),
        _idx(idx),
        _ordering(Ordering::make(_idx.keyPattern())),
        _startKey(),
        _endKey(),
        _endKeyInclusive(true),
        _multiKey(cl->isMultiKey(cl->idxNo(idx))),
        _direction(direction),
        _bounds(bounds),
        _boundsMustMatch(true),
        _nscanned(0),
        _nscannedObjects(0),
        _prelock(!cc().opSettings().getJustOne() && numWanted == 0),
        _tailable(false),
        _ok(false),
        _getf_iteration(0)
    {
        verify( _cl != NULL );
        verify( _bounds );
        _boundsIterator.reset( new FieldRangeVectorIterator( *_bounds , singleIntervalLimit ) );
        _boundsIterator->prepDive();
        _startKey = _bounds->startKey();
        _endKey = _bounds->endKey();
        _endKeyInclusive = _bounds->endKeyInclusive();
        TOKULOG(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        _cursor = idx.getCursor(cursor_flags());
        DBC* cursor = _cursor->dbc();
        cursor->c_set_check_interrupt_callback(cursor, cursor_check_interrupt, &_interrupt_extra);
        initializeDBC();

        // Fairly bad hack:
        //
        // Primary keys are not skipped properly when a non-inclusive start bound is specified.
        // See IndexCursor::skipToNextKey()
        //
        // Do a single advance here - the PK is unique so the next key is guaranteed to be
        // strictly greater than the start key. We have to play games with _nscanned because
        // advance()'s checkCurrentAgainstBounds() is going to increment it by 1 (we don't want that).
        if (ok() && _cl->isPKIndex(_idx) && !_bounds->startKeyInclusive() && _currKey == _startKey) {
            const long long oldNScanned = _nscanned;
            advance();
            verify(oldNScanned <= _nscanned);
            _nscanned = oldNScanned;
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

    bool IndexCursor::cursor_check_interrupt(void* extra) {
        ExceptionSaver *info = static_cast<ExceptionSaver *>(extra);
        try {
            killCurrentOp.checkForInterrupt(); // uasserts if we should stop
        } catch (const std::exception &ex) {
            info->saveException(ex);
            return true;
        }
        return false;
    }

    int IndexCursor::cursor_getf(const DBT *key, const DBT *val, void *extra) {
        struct cursor_getf_extra *info = static_cast<struct cursor_getf_extra *>(extra);
        try {
            if (key != NULL) {
                RowBuffer *buffer = info->buffer;
                storage::Key sKey(key);
                buffer->append(sKey, val->size > 0 ?
                        BSONObj(static_cast<const char *>(val->data)) : BSONObj());

                // request more bulk fetching if we are allowed to fetch more rows
                // and the row buffer is not too full.
                if (++info->rows_fetched < info->rows_to_fetch && !buffer->isGorged()) {
                    return TOKUDB_CURSOR_CONTINUE;
                }
            }
            return 0;
        } catch (const std::exception &ex) {
            info->saveException(ex);
        }
        return -1;
    }

    void IndexCursor::refreshMinUnsafeEndKey() {
        TailableCollection *cl = _cl->as<TailableCollection>();
        _endKey = cl->minUnsafeKey();
    }

    void IndexCursor::setTailable() {
        // tailable cursors may not be created over secondary indexes,
        // and they must intend to read to the end of the collection.
        verify( _cl->isPKIndex(_idx) );
        verify( _endKey.isEmpty() || _endKey == maxKey );
        // mark the cursor as tailable and set the end key bound tothe minimum unsafe
        // key to read from the namespace, non-inclusive.
        _tailable = true;
        refreshMinUnsafeEndKey();
        _endKeyInclusive = false;
        // Tailable cursors _must_ use endKey/endKeyInclusive so the bounds we
        // may or may not have gotten via the constructor is no longer valid.
        _bounds.reset();
        checkCurrentAgainstBounds();
    }

    bool IndexCursor::forward() const {
        return _direction > 0;
    }

    void IndexCursor::_prelockRange(const BSONObj &startKey, const BSONObj &endKey) {
        const bool isSecondary = !_cl->isPKIndex(_idx);

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

        DBC *cursor = _cursor->dbc();
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
        checkCurrentAgainstBounds();
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
        const bool isSecondary = !_cl->isPKIndex(_idx);
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
        struct cursor_getf_extra extra(&_buffer, rows_to_fetch);
        DBC *cursor = _cursor->dbc();
        if ( forward() ) {
            r = cursor->c_getf_set_range(cursor, getf_flags(), &key_dbt, cursor_getf, &extra);
        } else {
            r = cursor->c_getf_set_range_reverse(cursor, getf_flags(), &key_dbt, cursor_getf, &extra);
        }
        if (r == -1) {
            extra.throwException();
            msgasserted(17325, "got -1 from getf callback but no exception saved");
        }
        if (r == TOKUDB_INTERRUPTED) {
            _interrupt_extra.throwException();
        }
        if ( r != 0 && r != DB_NOTFOUND ) {
            extra.throwException();
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
            // If nscanned is increased by more than 20 before a matching key is found, abort
            // skipping through the index to find a matching key.  This iteration cutoff
            // prevents unbounded internal iteration within IndexCursor::initializeDBC and
            // IndexCursor::advance(). See SERVER-3448.
            const long long startNscanned = _nscanned;
            if ( skipOutOfRangeKeysAndCheckEnd() ) {
                do {
                    if ( _nscanned > startNscanned + 20 ) {
                        // If iteration is aborted before a key matching _bounds is identified, the
                        // cursor may be left pointing at a key that is not within bounds
                        // (_bounds->matchesKey( currKey() ) may be false).  Set _boundsMustMatch to
                        // false accordingly.
                        _boundsMustMatch = false;
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
                    forward() ? b.appendMinKey( "" ) : b.appendMaxKey( "" );
                } else {
                    // Regular ascending order. Max key skips forward.
                    forward() ? b.appendMaxKey( "" ) : b.appendMinKey( "" );
                }
            }
        }

        // This differs from findKey in that we set PK to max to move forward and min
        // to move backward, resulting in a "skip" of the key prefix, not a "find".
        const bool isSecondary = !_cl->isPKIndex(_idx);
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

    // Check if the current key is beyond endKey.
    void IndexCursor::checkEnd() {
        if ( !ok() ) {
            return;
        }
        if ( !_endKey.isEmpty() ) {
            const int cmp = _endKey.woCompare( _currKey, _ordering );
            const int sign = cmp == 0 ? 0 : (cmp > 0 ? 1 : -1);
            if ( (sign != 0 && sign != _direction) || (sign == 0 && !_endKeyInclusive) ) {
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
        DBC *cursor = _cursor->dbc();
        if ( forward() ) {
            r = cursor->c_getf_next(cursor, getf_flags(), cursor_getf, &extra);
        } else {
            r = cursor->c_getf_prev(cursor, getf_flags(), cursor_getf, &extra);
        }
        if (r == -1) {
            extra.throwException();
            msgasserted(17326, "got -1 from getf callback but no exception saved");
        }
        if (r == TOKUDB_INTERRUPTED) {
            _interrupt_extra.throwException();
        }
        if ( r != 0 && r != DB_NOTFOUND ) {
            extra.throwException();
            storage::handle_ydb_error(r);
        }

        _getf_iteration++;
        return extra.rows_fetched > 0 ? true : false;
    }

    void IndexCursor::_advance() {
        // Reset this flag at the start of a new iteration.
        // See IndexCursor::checkCurrentAgainstBounds()
        _boundsMustMatch = true;

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
            TOKULOG(3) << "_advance moved to K, PK, Obj" << _currKey << _currPK << _currObj << endl;
        } else {
            TOKULOG(3) << "_advance exhausted" << endl;
        }
    }

    bool IndexCursor::advance() {
        killCurrentOp.checkForInterrupt();
        if ( ok() ) {
            // Advance one row further, and then check if we've went out of bounds.
            _advance();
        } else {
            if ( tailable() ) {
                if ( _currKey < _endKey ) {
                    // Read the most up-to-date minUnsafeKey from the namespace
                    refreshMinUnsafeEndKey();
                    _advance();
                } else {
                    // reset _currKey, we may have accidentally
                    // gone past _endKey when we did our last advance
                    // and saw something we are not allowed to see.
                    _currKey = _endKey;
                    // Read the most up-to-date minUnsafeKey from the namespace
                    refreshMinUnsafeEndKey();
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
            _nscannedObjects++;
            bool found = _cl->findByPK( _currPK, _currObj );
            if ( !found ) {
                // If we didn't find the associated object, we must be either:
                // - a snapshot transaction whose context deleted the current pk
                // - a read uncommitted cursor with stale data
                // In either case, we may advance and try again exactly once.
                TOKULOG(4) << "current() did not find associated object for pk " << _currPK << endl;
                advance();
                if ( ok() ) {
                    found = _cl->findByPK( _currPK, _currObj );
                    uassert( 16741, str::stream()
                                << toString() << ": could not find associated document with pk "
                                << _currPK << ", index key " << _currKey, found );
                }
            }
        }
        bool shouldAppendPK = _cl->isCapped() && cc().opSettings().shouldCappedAppendPK();
        if (shouldAppendPK) {
            BSONObjBuilder b;
            b.appendElements(_currObj);
            b.append("$_", _currPK);
            return b.obj();
        }
        return _currObj;
    }

    bool IndexCursor::currentMatches( MatchDetails *details ) {
         // If currKey() might not match the specified _bounds, check whether or not it does.
         if ( !_boundsMustMatch && _bounds && !_bounds->matchesKey( currKey() ) ) {
             // If the key does not match _bounds, it does not match the query.
             return false;
         }
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

} // namespace mongo
