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

    bool BasicCursor::advance() {
#if 0
        killCurrentOp.checkForInterrupt();
        if ( eof() ) {
            if ( tailable_ && !last.isNull() ) {
                curr = s->next( last );
            }
            else {
                return false;
            }
        }
        else {
            last = curr;
            curr = s->next( curr );
        }
#endif
        ::abort();
        incNscanned();
        return ok();
    }

#if 0
    /* these will be used outside of mutexes - really functors - thus the const */
    class Forward : public AdvanceStrategy {
        virtual DiskLoc next( const DiskLoc &prev ) const {
            ::abort(); return minDiskLoc; //return prev.rec()->getNext( prev );
        }
    } _forward;

    class Reverse : public AdvanceStrategy {
        virtual DiskLoc next( const DiskLoc &prev ) const {
            ::abort(); return minDiskLoc; //return prev.rec()->getPrev( prev );
        }
    } _reverse;

    const AdvanceStrategy *forward() {
        return &_forward;
    }
    const AdvanceStrategy *reverse() {
        return &_reverse;
    }
#endif

    ForwardCappedCursor* ForwardCappedCursor::make( NamespaceDetails* nsd /*, const DiskLoc& startLoc */ ) {
#if 0
        auto_ptr<ForwardCappedCursor> ret( new ForwardCappedCursor( nsd ) );
        ret->init( startLoc );
        return ret.release();
#endif
        ::abort();
        return NULL;
    }

    ForwardCappedCursor::ForwardCappedCursor( NamespaceDetails* _nsd ) :
        nsd( _nsd ) {
    }

#if 0
    void ForwardCappedCursor::init( const DiskLoc& startLoc ) {
        if ( !nsd )
            return;
        DiskLoc start = startLoc;
        if ( start.isNull() ) {
            ::abort();
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
#endif

#if 0
    DiskLoc ForwardCappedCursor::next( const DiskLoc &prev ) const {
        verify( nsd );
        ::abort();
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
#endif

    ReverseCappedCursor::ReverseCappedCursor( NamespaceDetails *_nsd /*, const DiskLoc &startLoc*/ ) :
        nsd( _nsd ) {
        if ( !nsd )
            return;
#if 0
        DiskLoc start = startLoc;
        if ( start.isNull() ) {
            ::abort();
            if ( !nsd->capLooped() ) {
                start = nsd->lastRecord();
            }
            else {
                start = nsd->capExtent.ext()->lastRecord;
                ::abort();
            }
        }
        curr = start;
        ::abort();
        s = this;
#endif
        incNscanned();
    }

#if 0
    DiskLoc ReverseCappedCursor::next( const DiskLoc &prev ) const {
        verify( nsd );
        ::abort();
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

        ::abort();
        return i;
    }
#endif
} // namespace mongo
