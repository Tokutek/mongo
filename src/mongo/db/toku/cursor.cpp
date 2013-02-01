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

#include "pch.h"

#include "db/toku/env.h"
#include "db/toku/cursor.h"
#include "db/toku/dbt-bson-inl.h"
#include "db/toku/invariant.h"
#include "db/toku/row_buffer.h"

namespace mongo {

    TokuDBCursor::TokuDBCursor(NamespaceDetails *nsd, int idxNo, const IndexDetails &idx) :
        BtreeCursor(nsd, idxNo, idx),
        cursor(NULL),
        txn(NULL),
        row_buffer(),
        bulk_fetch_iteration(0) {
    }

    // close the cursor and maybe commit the txn when the cursor is destroyed
    TokuDBCursor::~TokuDBCursor() {
        if (cursor) {
            int r = cursor->c_close(cursor);
            invariant(r == 0);
            toku::env_maybe_commit_txn(txn);
        }
    }

    // if either key or recordloc are non null inside the get callback,
    // set them to the values just read from the db
    struct cursor_getf_cb_extra {
        RowBuffer *row_buffer;
        int rows_fetched;
        int rows_to_fetch;
    };

    // how many rows should we fetch for a particular iteration?
    // crude performance testing shows us that it is quite expensive 
    // to go into go into ydb layer and start bulk fetching. we should
    // fetch exponentially more and more rows with each iteration so
    // that we avoid too much ydb layer overhead without reading it
    // all at once, which is too aggressive for queries that might
    // not want all of it.
    static int max_rows_to_fetch(int iteration) {
        int rows_to_fetch = 1;
        switch (iteration) {
            case 0:
                rows_to_fetch = 1;
                break;
            case 1:
                rows_to_fetch = 64;
                break;
            case 2:
                rows_to_fetch = 256;
                break;
            case 3:
                rows_to_fetch = 1024;
                break;
            default:
                rows_to_fetch = 4096;
                break;
        }
        return rows_to_fetch;
    }

    // ydb layer cursor callback
    static int cursor_getf_cb(const DBT *index_key, const DBT *val, void *extra) {
        int r = 0;

        // the cursor callback is called even if the desired
        // key is not found. in that case, index_key == NULL
        if (index_key) {
            struct cursor_getf_cb_extra *info = (struct cursor_getf_cb_extra *) extra;
            RowBuffer *row_buffer = info->row_buffer;
            
            // put this row into the row buffer unconditionally
            BSONObj row_key = toku::init_bson_from_dbt(index_key);
            DiskLoc row_loc = toku::init_diskloc_from_dbt(index_key);
            BSONObj row_obj = toku::init_bson_from_dbt(val);
            row_buffer->append(row_key, row_loc, row_obj);
            info->rows_fetched++;

            // request more bulk fetching if we are allowed to fetch more rows
            // and the row buffer is not too full.
            if (info->rows_fetched < info->rows_to_fetch && !row_buffer->isGorged()) {
                r = TOKUDB_CURSOR_CONTINUE;
            }
        }

        return r;
    }

    // set the cursor to be the first key >= given key
    // return:
    //      true, the cursor was set successfully, may proceed
    //      false, the cursor could not be set, no key >= given
    // effect:
    //      if there is no such key, set the btree bucket to the null diskloc,
    //      so this cursor is marked as exhausted.
    bool TokuDBCursor::set_cursor(const BSONObj &key) {
        char key_buf[toku::index_key_size(startKey)];
        const mongo::DiskLoc zero_diskloc = mongo::DiskLoc(0, 0);
        DBT index_key = toku::generate_index_key(key_buf, startKey, zero_diskloc);

        int rows_to_fetch = max_rows_to_fetch(bulk_fetch_iteration);
        struct cursor_getf_cb_extra extra = { &row_buffer, 0, rows_to_fetch };
        int r = cursor->c_getf_set_range(cursor, 0, &index_key, cursor_getf_cb, &extra);
        invariant(r == 0 || r == DB_NOTFOUND);
        bulk_fetch_iteration++;
        return r == 0 ? true : false;
    }

    // bulk fetch from the cursor and store the rows into the buffer
    // return:
    //      true, more rows were read from tokudb and stored
    //      false, no more rows to read from the cursor
    bool TokuDBCursor::bulk_fetch_more_rows() {
        //static Timer timer;
        //timer.reset();
        int rows_to_fetch = max_rows_to_fetch(bulk_fetch_iteration);
        struct cursor_getf_cb_extra extra = { &row_buffer, 0, rows_to_fetch };
        int r = cursor->c_getf_next(cursor, 0, cursor_getf_cb, &extra);
        invariant(r == 0 || r == DB_NOTFOUND);
        bulk_fetch_iteration++;
        //printf("%s: finished, ret %d, iteration %d, _nscanned %lld, rows_fetched %d\n",
        //        __FUNCTION__, r, bulk_fetch_iteration, _nscanned, extra.rows_fetched);
        //printf("%s: fetched %d rows in %llu usecs\n",
        //        __FUNCTION__, extra.rows_fetched, timer.micros());
        return r == 0 ? true : false;
    }

    // initialize the tokudb cursor and position it over the start key
    void TokuDBCursor::init_internal(const BSONObj &startKey, const BSONObj &endKey) {
        // get a db, maybe a txn, and create a cursor over the db
        DB *db = toku::env_get_db_handle_by_idx(indexDetails);
        toku::env_maybe_begin_txn(&txn);
        int r = db->cursor(db, txn, &cursor, 0);
        invariant(r == 0);

        // see if this cursor is over a clustering index
        clustering = indexDetails.info.obj()["clustering"].trueValue();

        // TODO: prelock range from startKey to endKey to get prefetching
        // position the cursor over the first key >= startKey
        bool ok = set_cursor(startKey);
        bucket = ok ? minDiskLoc : DiskLoc(); 
    }

    // create cursor, given
    // - start, end key range
    // - ignore the others
    void TokuDBCursor::init(const BSONObj &startKey, const BSONObj &endKey, bool endKeyInclusive, int direction) {
        // for some reason we're not allowed to do this in the constructor
        BtreeCursor::init(startKey, endKey, endKeyInclusive, direction);
        init_internal(startKey, endKey);
        invariant(cursor);
    }

    // create a cursor, given
    // - bounds: set of intervals ie (0, 1] that this cursor is valid over
    // - singleIntervalLimit: sounds like we're only allowed to use one iterval
    // - direction: do we iterate forward (using next) or backward (using prev)
    void TokuDBCursor::init(const shared_ptr<FieldRangeVector> &bounds, int singleIntervalLimit, int direction) {
        // for some reason we're not allowed to do this in the constructor
        BtreeCursor::init(bounds,singleIntervalLimit,direction );
        init_internal(startKey, endKey);
        invariant(cursor);
    }

    // useful function: return the associated diskloc for the current key
    DiskLoc TokuDBCursor::currLoc() {
        DiskLoc loc = row_buffer.currentLoc();
        return loc;
    }

    // get the associated document for the cursor's current position.
    //
    // if we're using a clustered index, then we can return the current
    // object for this cursor since we already have the data.
    //
    // if not, do it the way the btreecursor does - make a bsonobj by
    // reading from the data file heap (potentially causes IO)
    BSONObj TokuDBCursor::current() {
        BSONObj obj;
        obj = row_buffer.currentObj();
        if (clustering) {
            // clustering indexes should store a non-empty obj
            invariant(!obj.isEmpty());
        } else {
            // non-clustering indexes should store an empty obj
            invariant(obj.isEmpty());
            //obj = BSONObj::make(_current()); 
            ::abort();
        }
        return obj;
    }

    // it appears this is only used by the btree cursor code for 
    // checkLocation(), which we don't support, so we do nothing here.
    BSONObj TokuDBCursor::keyAt(int ofs) const {
        return BSONObj();
    }

    // get the current key for this cursor. it's probably okay to cache this 
    // for the lifetime of a call to advance(), but no later, since writes 
    // can happen in between calls to advance().
    BSONObj TokuDBCursor::currKey() const {
        BSONObj obj = row_buffer.currentKey();
        return obj;
    }

    // apparently only used in unit tests. unsupported.
    bool TokuDBCursor::curKeyHasChild() {
        return false;
    }

    // returns: true if some keys were skipped, false otherwise.
    // we don't need to skip keys because the tokudb cursor isn't
    // going to give back stale data (ie: data that was deleted).
    // the mongo cursor could so it uses this to skip them.
    // we never skip so we always return false.
    bool TokuDBCursor::skipUnusedKeys() {
        return false;
    }

    //
    // BtreeCursor specific stuff declared pure virtual
    //

    // this sets keyOfs to something. we don't care. set to zero, do nothing.
    void TokuDBCursor::_advanceTo(DiskLoc &thisLoc, int &keyOfs, const BSONObj &keyBegin,
            int keyBeginLen, bool afterKey, const vector< const BSONElement * > &keyEnd,
            const vector< bool > &keyEndInclusive, const Ordering &order, int direction ) {
    }

    // move the cursor in the given direction.
    // tokudb cursors do not care about the keyofs. so don't touch it.
    // we should return a non null diskloc if we found the next key,
    // or the null diskloc if we couldn't.
    DiskLoc TokuDBCursor::_advance(const DiskLoc& thisLoc, int& keyOfs, int direction, const char *caller) {
        invariant(cursor);
        // try to move the row buffer to the next row
        bool ok = row_buffer.next();
        if (!ok) {
            // no more rows in the buffer. empty it and try to get more.
            // if we can't, set bucket = null diskloc, invalidating it.
            row_buffer.empty();
            ok = bulk_fetch_more_rows();
        }
        return ok ? minDiskLoc : DiskLoc(); 
    }

    // this doesn't look very interesting. unsupported.
    void TokuDBCursor::_audit() {
    }

    // look for an exact key, loc match in the index. if not found,
    // return an invalid diskloc. the caller will invalidate the cursor.
    DiskLoc TokuDBCursor::_locate(const BSONObj& key, const DiskLoc& loc) {
        bool ok = set_cursor(key);
        return ok && currLoc() == loc ? minDiskLoc : DiskLoc();
    }
} /* namespace mongo */
