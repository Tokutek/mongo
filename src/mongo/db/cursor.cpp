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
#include "mongo/db/namespace_details.h"

namespace mongo {

    static BSONObj &_minOrMaxKey(const BSONObj &keyPattern,
                               const int direction, const bool startKey) {
        // Since minKey/maxKey are single field keys, they will only be compared
        // to the first field in any index key. That means only the first field's
        // ordering matters. Here, descending(1) means mask the ordering bits with 1.
        const bool ascending = !Ordering::make(keyPattern).descending(1);
        const bool forward = direction > 0;
        if (ascending) {
            return forward ? (startKey ? minKey : maxKey)  // ascending forward starts min, ends max.
                           : (startKey ? maxKey : minKey); // ascending reverse starts max, ends min.
        } else {
            return forward ? (startKey ? maxKey : minKey)  // descending forward starts max, ends min.
                           : (startKey ? minKey : maxKey); // descending reverse starts min, ends max.
        }
    }

    IndexScanCursor::IndexScanCursor( NamespaceDetails *d, const IndexDetails &idx,
                                      int direction, int numWanted ) :
        IndexCursor( d, idx,
                     _minOrMaxKey(idx.keyPattern(), direction, true),
                     _minOrMaxKey(idx.keyPattern(), direction, false),
                     true, direction, numWanted ) {
    }

    Cursor *BasicCursor::make( NamespaceDetails *d, int direction ) {
        if ( d != NULL ) {
            return new BasicCursor(d, direction);
        } else {
            return new DummyCursor(direction);
        }
    }

    BasicCursor::BasicCursor( NamespaceDetails *d, int direction ) :
        IndexScanCursor( d, d->getPKIndex(), direction ) {
    }

} // namespace mongo
