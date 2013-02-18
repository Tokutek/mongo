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

#if 0
    // TODO: Phase this class out in favor of an IndexCursor over the _id index.

    struct cursor_getf_extra {
        BSONObj *const key;
        BSONObj *const val;
        cursor_getf_extra(BSONObj *const k, BSONObj *const v) : key(k), val(v) { }
    };

    static int cursor_getf(const DBT *key, const DBT *val, void *extra) {
        struct cursor_getf_extra *info = static_cast<struct cursor_getf_extra *>(extra);
        dassert(key != NULL);
        dassert(val != NULL);

        BSONObj keyObj(static_cast<char *>(key->data));
        BSONObj valObj(static_cast<char *>(val->data));
        dassert(keyObj.objsize() == (int) key->size);
        dassert(valObj.objsize() == (int) val->size);
        *info->key = keyObj.getOwned();
        *info->val = valObj.getOwned();
        return 0;
    }

    BasicCursor::BasicCursor(NamespaceDetails *nsd, int direction)
        : _nsd(nsd), _direction(direction), _cursor(NULL),  _nscanned(0) {

        if (_nsd != NULL) {
            // Get a cursor over the _id index
            int idxNo = nsd->findIdIndex();
            IndexDetails &id = nsd->idx(idxNo);
            _cursor = id.cursor();

            // Get the first/last element depending on direction
            struct cursor_getf_extra extra(&_currKey, &_currObj);
            int r;
            if (_direction > 0) {
                r = _cursor->c_getf_first(_cursor, 0, cursor_getf, &extra);
            } else {
                r = _cursor->c_getf_last(_cursor, 0, cursor_getf, &extra);
            }
            DEV {
                if (r == 0) {
                    verify(ok());
                } else {
                    verify(!ok());
                }
            }
            incNscanned();
        }
        init();
    }

    BasicCursor::~BasicCursor() {
        if (_cursor != NULL) {
            int r = _cursor->c_close(_cursor);
            verify(r == 0);
        }
    }

    bool BasicCursor::advance() {
        killCurrentOp.checkForInterrupt();
        if ( eof() ) {
            // TODO: Tailable cursors
            if ( _tailable && /* !last.isNull() */ false ) {
#if 0
                curr = s->next( last );
#endif
            }
            else {
                return false;
            }
            return false;
        }
        else {
            // Reset current key/obj to empty.
            _currKey = BSONObj();
            _currObj = BSONObj();

            int r;
            struct cursor_getf_extra extra(&_currKey, &_currObj);
            if (_direction > 0) {
                r = _cursor->c_getf_next(_cursor, 0, cursor_getf, &extra);
            } else {
                r = _cursor->c_getf_prev(_cursor, 0, cursor_getf, &extra);
            }

            DEV {
                if (r == 0) {
                    verify(ok());
                } else {
                    verify(!ok());
                }
            }
        }
        incNscanned();
        return ok();
    }
#endif

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
