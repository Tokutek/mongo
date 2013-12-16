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
#include "mongo/db/kill_current_op.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/collection.h"

namespace mongo {

    //
    // The centralized factories for creating cursors over collections.
    //

    shared_ptr<Cursor> Cursor::make(CollectionData *cd, const int direction) {
        return shared_ptr<Cursor>(new BasicCursor(cd, direction));
    }

    shared_ptr<Cursor> Cursor::make(Collection *cl, const int direction,
                                    const bool countCursor) {
        if (cl != NULL) {
            CollectionData* cd = cl->as<CollectionData>();
            if (countCursor) {
                return shared_ptr<Cursor>(new IndexScanCountCursor(cd, cl->findSmallestOneToOneIndex()));
            } else {
                return shared_ptr<Cursor>(new BasicCursor(cd, direction));
            }
        } else {
            return shared_ptr<Cursor>(new DummyCursor(direction));
        }
    }

    shared_ptr<Cursor> Cursor::make(Collection *cl, const IndexDetails &idx,
                                    const int direction,
                                    const bool countCursor) {
        CollectionData* cd = cl->as<CollectionData>();
        if (countCursor) {
            return shared_ptr<Cursor>(new IndexScanCountCursor(cd, idx));
        } else {
            return shared_ptr<Cursor>(new IndexScanCursor(cd, idx, direction));
        }
    }

    shared_ptr<Cursor> Cursor::make(CollectionData *cd, const IndexDetails &idx,
                                    const int direction) {
        return shared_ptr<Cursor>(new IndexScanCursor(cd, idx, direction));
    }

    shared_ptr<Cursor> Cursor::make(Collection *cl, const IndexDetails &idx,
                                    const BSONObj &startKey, const BSONObj &endKey,
                                    const bool endKeyInclusive,
                                    const int direction, const int numWanted,
                                    const bool countCursor) {
        CollectionData* cd = cl->as<CollectionData>();
        if (countCursor) {
            return shared_ptr<Cursor>(new IndexCountCursor(cd, idx, startKey, endKey,
                                                           endKeyInclusive));
        } else {
            return shared_ptr<Cursor>(new IndexCursor(cd, idx, startKey, endKey,
                                                      endKeyInclusive, direction, numWanted));
        }
    }

    shared_ptr<Cursor> Cursor::make(CollectionData *cd, const IndexDetails &idx,
                                    const BSONObj &startKey, const BSONObj &endKey,
                                    const bool endKeyInclusive,
                                    const int direction, const int numWanted) {
        return shared_ptr<Cursor>(new IndexCursor(cd, idx, startKey, endKey,
                                                  endKeyInclusive, direction, numWanted));
    }

    shared_ptr<Cursor> Cursor::make(Collection *cl, const IndexDetails &idx,
                                    const shared_ptr<FieldRangeVector> &bounds,
                                    const int singleIntervalLimit,
                                    const int direction, const int numWanted,
                                    const bool countCursor) {
        CollectionData* cd = cl->as<CollectionData>();
        if (countCursor) {
            return shared_ptr<Cursor>(new IndexCountCursor(cd, idx, bounds));
        } else {
            return shared_ptr<Cursor>(new IndexCursor(cd, idx, bounds,
                                                      singleIntervalLimit, direction, numWanted));
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    bool ScanCursor::reverseMinMaxBoundsOrder(const Ordering &ordering, const int direction) {
        // Only the first field's direction matters, because this function is only called
        // to possibly reverse bounds ordering with min/max key, which is single field.
        const bool ascending = !ordering.descending(1);
        const bool forward = direction > 0;
        // We need to reverse the order if exactly one of the query or the index are descending.  If
        // both are descending, the normal order is fine.
        return ascending != forward;
    }

    const BSONObj &ScanCursor::startKey(const BSONObj &keyPattern, const int direction) {
        // Scans intuitively start at minKey, but may need to be reversed to maxKey.
        return reverseMinMaxBoundsOrder(Ordering::make(keyPattern), direction) ? maxKey : minKey;
    }

    const BSONObj &ScanCursor::endKey(const BSONObj &keyPattern, const int direction) {
        // Scans intuitively end at maxKey, but may need to be reversed to minKey.
        return reverseMinMaxBoundsOrder(Ordering::make(keyPattern), direction) ? minKey : maxKey;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    IndexScanCursor::IndexScanCursor( CollectionData *cl, const IndexDetails &idx,
                                      int direction, int numWanted ) :
        IndexCursor( cl, idx,
                     ScanCursor::startKey(idx.keyPattern(), direction),
                     ScanCursor::endKey(idx.keyPattern(), direction),
                     true, direction, numWanted ) {
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    BasicCursor::BasicCursor( CollectionData *cl, int direction ) :
        IndexScanCursor( cl, cl->getPKIndex(), direction ) {
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    IndexCountCursor::IndexCountCursor( CollectionData *cl, const IndexDetails &idx,
                                        const BSONObj &startKey, const BSONObj &endKey,
                                        const bool endKeyInclusive ) :
        IndexCursor(cl, idx, startKey, endKey, endKeyInclusive, 1, 0),
        _bufferedRowCount(0),
        _exhausted(false),
        _endSKeyPrefix(_endKey, NULL) {
        TOKULOG(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        checkAssumptionsAndInit();
    }

    IndexCountCursor::IndexCountCursor( CollectionData *cl, const IndexDetails &idx,
                                        const shared_ptr< FieldRangeVector > &bounds ) :
        // This will position the cursor correctly using bounds, which will do the right
        // thing based on bounds->start/endKey() and bounds->start/endKeyInclusive()
        IndexCursor(cl, idx, bounds, false, 1, 0),
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

    IndexScanCountCursor::IndexScanCountCursor( CollectionData *cl, const IndexDetails &idx ) :
        IndexCountCursor( cl, idx,
                          ScanCursor::startKey(idx.keyPattern(), 1), 
                          ScanCursor::endKey(idx.keyPattern(), 1),
                          true ) {
        verify(forward());
    }

} // namespace mongo
