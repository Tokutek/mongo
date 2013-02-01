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

#ifndef TOKUDB_ROW_BUFFER_H
#define TOKUDB_ROW_BUFFER_H

#include "mongo/bson/bsonobj.h"
#include "mongo/db/diskloc.h"

namespace mongo {

    class RowBuffer {
    public:
        RowBuffer();

        ~RowBuffer();

        // append the given key, loc and obj to the end of the buffer
        void append(const BSONObj &key, const DiskLoc &loc, const BSONObj &obj);

        // the row buffer is gorged if its current size is greater
        // than or equal to the preferred size.
        // return:
        //      true, buffer is gorged
        //      false, buffer could fit more data
        // rationale:
        //      - if true, then it makes more sense to empty and refill
        //      the buffer than trying to stuff more in it.
        //      - if false, then an append probably won't cause a realloc,
        //      so go ahead and do it.
        bool isGorged() const;

        // move the internal buffer position to the next key, loc pair
        // returns:
        //      true, the buffer is reading to be read via current()
        //      false, the buffer has no more data
        bool next();

        // get the current key from the buffer.
        BSONObj currentKey() const;

        // get the current diskloc from the buffer.
        DiskLoc currentLoc() const;

        // get the current obj from the buffer.
        BSONObj currentObj() const;

        // empty the row buffer, resetting all data and internal positions
        void empty();

    private:
        // store rows in a buffer that has a "preferred size". if we need to 
        // fit more in the buf, then it's okay to go over. _size captures the
        // real size of the buffer.
        // _end_offset is where we will write new bytes for append(). it is
        // modified and advanced after the append.
        // _current_offset is where we will read for current(). it is modified
        // and advanced after a next()
        static const size_t _BUF_SIZE_PREFERRED = 128 * 1024;
        char *_buf;
        size_t _size;
        size_t _end_offset;
        size_t _current_offset;
    };

} /* namespace mongo */

#endif /* TOKUDB_ROW_BUFFER_H  */
