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
#include "mongo/db/collection.h"

namespace mongo {

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

    IndexScanCursor::IndexScanCursor( Collection *cl, const IndexDetails &idx,
                                      int direction, int numWanted ) :
        IndexCursor( cl, idx,
                     ScanCursor::startKey(idx.keyPattern(), direction),
                     ScanCursor::endKey(idx.keyPattern(), direction),
                     true, direction, numWanted ) {
    }

    void IndexScanCursor::checkEnd() {
        // Nothing to do in the normal case. "Scan" cursors always iterate over
        // the whole index, so the entire keyspace is in bounds.
        DEV {
            verify(!_endKey.isEmpty());
            const int cmp = _endKey.woCompare( _currKey, _ordering );
            const int sign = cmp == 0 ? 0 : (cmp > 0 ? 1 : -1);
            if ( (sign != 0 && sign != _direction) || (sign == 0 && !_endKeyInclusive) ) {
                msgasserted(17202, "IndexScanCursor has a bad currKey/endKey combination");
            }
        }
    }

    shared_ptr<Cursor> BasicCursor::make( Collection *cl, int direction ) {
        if ( cl != NULL ) {
            return shared_ptr<Cursor>(new BasicCursor(cl, direction));
        } else {
            return shared_ptr<Cursor>(new DummyCursor(direction));
        }
    }

    BasicCursor::BasicCursor( Collection *cl, int direction ) :
        IndexScanCursor( cl, cl->getPKIndex(), direction ) {
    }

} // namespace mongo
