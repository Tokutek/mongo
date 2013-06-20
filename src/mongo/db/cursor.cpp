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

    const BSONObj &IndexScanCursor::startKey(const BSONObj &keyPattern, const int direction) {
        // Scans intuitively start at minKey, but may need to be reversed to maxKey.
        return reverseMinMaxBoundsOrder(Ordering::make(keyPattern), direction) ? maxKey : minKey;
    }

    const BSONObj &IndexScanCursor::endKey(const BSONObj &keyPattern, const int direction) {
        // Scans intuitively end at maxKey, but may need to be reversed to minKey.
        return reverseMinMaxBoundsOrder(Ordering::make(keyPattern), direction) ? minKey : maxKey;
    }

    IndexScanCursor::IndexScanCursor( NamespaceDetails *d, const IndexDetails &idx,
                                      int direction, int numWanted ) :
        IndexCursor( d, idx,
                     startKey(idx.keyPattern(), direction),
                     endKey(idx.keyPattern(), direction),
                     true, direction, numWanted ) {
    }

    shared_ptr<Cursor> BasicCursor::make( NamespaceDetails *d, int direction ) {
        if ( d != NULL ) {
            return shared_ptr<Cursor>(new BasicCursor(d, direction));
        } else {
            return shared_ptr<Cursor>(new DummyCursor(direction));
        }
    }

    BasicCursor::BasicCursor( NamespaceDetails *d, int direction ) :
        IndexScanCursor( d, d->getPKIndex(), direction ) {
    }

} // namespace mongo
