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
#include "mongo/db/cursor.h"
#include "mongo/db/namespace_details.h"

namespace mongo {

    Cursor *BasicCursor::make( NamespaceDetails *d, int direction ) {
        if ( d != NULL ) {
            return new BasicCursor(d, direction);
        } else {
            return new DummyCursor(direction);
        }
    }

    BasicCursor::BasicCursor( NamespaceDetails *d, int direction ) :
        _c( d, d->getPKIndex(),
            direction > 0 ? minKey : maxKey, // start at the beginning for forward cursor
            direction > 0 ? maxKey : minKey, // finish at the end for forward cursor
            true, // end key is inclusive, because we want to scan everything.
            direction ),
        _direction(direction) {
    }

    Cursor *TailableCursor::make( NamespaceDetails *d ) {
        if ( d != NULL ) {
            return new TailableCursor(d);
        } else {
            return BasicCursor::make( d, 1 ); // this will make a dummy cursor
        }
    }

    TailableCursor::TailableCursor( NamespaceDetails *d ) :
        IndexCursor( d, d->getPKIndex(),
                     // iterate forward over the interval [ minKey, safeKey )
                     minKey, d->maxSafeKey(), false, 1 ) {
    }

    // pre/post condition: the current key is not passed the end key
    bool TailableCursor::advance() {
        killCurrentOp.checkForInterrupt();
        if ( ok() ) {
            // the last row we read was valid, move forward blindly.
            _advance();
        } else {
            _endKey = _d->maxSafeKey();
            findKey( !_currKey.isEmpty() ? _currKey : minKey );
        }
        // the key we are now positioned over may or may not be ok to read.
        // checkCurrentAgainstBounds() will decide based on the _endKey
        // (initialized to d->maxSafeKey), non-inclusive.
        return checkCurrentAgainstBounds();
    }

} // namespace mongo
