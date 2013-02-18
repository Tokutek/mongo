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

#include "pch.h"
#include "curop.h"

#include "mongo/db/cursor.h"
#include "mongo/db/namespace_details.h"

namespace mongo {

    extern BSONObj minKey;
    extern BSONObj maxKey;

    BasicCursor::BasicCursor(NamespaceDetails *d, int direction)
        : _c(d, d != NULL ? &d->idx(d->findIdIndex()) : NULL, // pass null for idx if no ns details
             direction > 0 ? minKey : maxKey, // start at the beginning for forward cursor
             direction > 0 ? maxKey : minKey, // finish at the end for forward cursor
             true, // end key is inclusive, because we want to scan everything.
             direction) {
  }

    // TODO: Capped collections
#if 0
    ForwardCappedCursor* ForwardCappedCursor::make( NamespaceDetails* nsd /*, const DiskLoc& startLoc */ ) {
        auto_ptr<ForwardCappedCursor> ret( new ForwardCappedCursor( nsd ) );
        ret->init( startLoc );
        return ret.release();
        return NULL;
    }

    ForwardCappedCursor::ForwardCappedCursor( NamespaceDetails* _nsd ) : BasicCursor( _nsd ) {
    }

    void ForwardCappedCursor::init( const DiskLoc& startLoc ) {
        if ( !nsd )
            return;
        DiskLoc start = startLoc;
        if ( start.isNull() ) {
            if ( !nsd->capLooped() )
                start = nsd->firstRecord();
            else {
                start = nsd->capExtent.ext()->firstRecord;
                if ( !start.isNull() && start == nsd->capFirstNewRecord ) {
                    start = nsd->capExtent.ext()->lastRecord;
                    start = nextLoop( nsd, start );
                }
            }
        }
        curr = start;
        s = this;
        incNscanned();
    }

    DiskLoc ForwardCappedCursor::next( const DiskLoc &prev ) const {
        verify( nsd );
        return minDiskLoc; // TODO: Redo this class
        if ( !nsd->capLooped() )
            return forward()->next( prev );

        DiskLoc i = prev;
        // Last record
        if ( i == nsd->capExtent.ext()->lastRecord )
            return DiskLoc();
        i = nextLoop( nsd, i );
        // If we become capFirstNewRecord from same extent, advance to next extent.
        if ( i == nsd->capFirstNewRecord &&
                i != nsd->capExtent.ext()->firstRecord )
            i = nextLoop( nsd, nsd->capExtent.ext()->lastRecord );
        // If we have just gotten to beginning of capExtent, skip to capFirstNewRecord
        if ( i == nsd->capExtent.ext()->firstRecord )
            i = nsd->capFirstNewRecord;
        return i;
    }

    ReverseCappedCursor::ReverseCappedCursor( NamespaceDetails *nsd /*, const DiskLoc &startLoc*/ ) :
        _nsd( nsd ) {
        if ( !_nsd )
            return;
        DiskLoc start = startLoc;
        if ( start.isNull() ) {
            if ( !nsd->capLooped() ) {
                start = nsd->lastRecord();
            }
            else {
                start = nsd->capExtent.ext()->lastRecord;
            }
        }
        curr = start;
        s = this;
        incNscanned();
    }

    DiskLoc ReverseCappedCursor::next( const DiskLoc &prev ) const {
        verify( nsd );
        return minDiskLoc;
        if ( !nsd->capLooped() )
            return reverse()->next( prev );

        DiskLoc i = prev;
        // Last record
        if ( nsd->capFirstNewRecord == nsd->capExtent.ext()->firstRecord ) {
            if ( i == nextLoop( nsd, nsd->capExtent.ext()->lastRecord ) ) {
                return DiskLoc();
            }
        }
        else {
            if ( i == nsd->capExtent.ext()->firstRecord ) {
                return DiskLoc();
            }
        }
        // If we are capFirstNewRecord, advance to prev extent, otherwise just get prev.
        if ( i == nsd->capFirstNewRecord )
            i = prevLoop( nsd, nsd->capExtent.ext()->firstRecord );
        else
            i = prevLoop( nsd, i );
        // If we just became last in cap extent, advance past capFirstNewRecord
        // (We know capExtent.ext()->firstRecord != capFirstNewRecord, since would
        // have returned DiskLoc() earlier otherwise.)
        if ( i == nsd->capExtent.ext()->lastRecord )
            i = reverse()->next( nsd->capFirstNewRecord );

        return i;
    }
#endif
} // namespace mongo
