/**
*    Copyright (C) 2012 Tokutek Inc.
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

#ifndef TOKUDB_CURSOR_H
#define TOKUDB_CURSOR_H

#include <tokudb.h>

#include "db/btree.h"
#include "db/toku/invariant.h"
#include "db/toku/row_buffer.h"

namespace mongo {

    // A TokuDB cursor extends a BtreeCursor's public interface.
    // Internally, it uses a real tokudb cursor and it caches the
    // last read row for speed.
    class TokuDBCursor : public BtreeCursor {
    public:

        // must use the above factor for a new tokudb cursor
        TokuDBCursor(NamespaceDetails *nsd, int idxNo, const IndexDetails &idx);

        // create a cursor for a given range, given:
        // - start, end keys
        // - endKeyInclusve whether to fetch the end key. just assume true. caller can filter.
        // - direciton: not sure
        virtual void init(const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction);

        // create a cursor, given
        // - bounds: set of intervals ie (0, 1] that this cursor is valid over
        // - singleIntervalLimit: sounds like we're only allowed to use one iterval
        // - direction: not sure
        virtual void init(const shared_ptr<FieldRangeVector> &bounds, int singleIntervalLimit, int direction);

        // useful function: return the associated diskloc for the current key
        virtual DiskLoc currLoc();

        // it appears this is only used by the btree cursor code for 
        // checkLocation(), which we don't support, so we do nothing here.
        virtual BSONObj keyAt(int ofs) const;

        // get the current key for this cursor. it's probably okay to cache this 
        // for the lifetime of a call to advance(), but no later, since writes 
        // can happen in between calls to advance().
        virtual BSONObj currKey() const;

        // get the associated document for the cursor's current position.
        //
        // if we're using a clustered index, then we can return the current
        // object for this cursor since we already have the data.
        //
        // if not, do it the way the btreecursor does - make a bsonobj by
        // reading from the data file heap (potentially causes IO)
        virtual BSONObj current();

        // apparently only used in unit tests. unsupported.
        virtual bool curKeyHasChild();

        // returns: true if some keys were skipped, false otherwise.
        // we don't need to skip keys because the tokudb cursor isn't
        // going to give back stale data (ie: data that was deleted).
        // the mongo cursor could so it uses this to skip them.
        // we never skip so we always return false.
        virtual bool skipUnusedKeys();

        // this sets keyOfs to something. we don't care. set to zero, do nothing.
        void _advanceTo(DiskLoc &thisLoc, int &keyOfs, const BSONObj &keyBegin,
                int keyBeginLen, bool afterKey, const vector<const BSONElement *> &keyEnd,
                const vector<bool> &keyEndInclusive, const Ordering &order, int direction);

        // move the cursor in the given direction.
        // tokudb cursors do not care about the keyofs. so don't touch it.
        // we should return a non null diskloc if we found the next key,
        // or the null diskloc if we couldn't.
        virtual DiskLoc _advance(const DiskLoc& thisLoc, int& keyOfs, int direction, const char *caller);

        // this doesn't look very interesting. unsupported.
        virtual void _audit();

        // this is only used for checkLocation and in a cursor init function
        // that tries to set the current btree bucket, which is meaningless
        // for a tokudb cursor. the only thing that matters is that we return
        // the null recordloc if we can't find the key. 
        virtual DiskLoc _locate(const BSONObj& key, const DiskLoc& loc);

        // close the underlying tokudb cursor, if it exists
        virtual ~TokuDBCursor();

    private:
        // tokudb DB cursor and associated txn
        DBC *cursor;
        DB_TXN *txn;

        // row buffer to cache rows read from the ydb layer, using bulk fetch
        RowBuffer row_buffer;

        // initialize the tokudb cursor and position it over the start key
        void init_internal(const BSONObj &startKey, const BSONObj &endKey);

        // set the cursor to the first key >= given key
        // return:
        //      true, the cursor was set successfully, may proceed
        //      false, the cursor could not be set, no key >= given
        bool set_cursor(const BSONObj &key);

        // bulk fetch from the cursor and store the rows into the buffer
        // return:
        //      true, more rows were read from tokudb and stored
        //      false, no more rows to read from the cursor
        bool bulk_fetch_more_rows();

        // how many times did we bulk fetch into the buffer? this
        // lets us figure out an appropriate amount of data to 
        // fetch next time. the idea is we don't want to fetch
        // every single key between startKey and endKey into the
        // buffer right away, but rather get exponentially more each
        // time we need it.
        int bulk_fetch_iteration;

        // is this cursor over a clustering index? if so, the document
        // associated with each index row is stored with the row, so
        // reads don't have to go to the main data file.
        bool clustering;
    };
}

#endif /* TOKUDB_CURSOR_H */
