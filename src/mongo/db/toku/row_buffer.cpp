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

#include <db/toku/invariant.h>
#include <db/toku/row_buffer.h>

namespace mongo {

    RowBuffer::RowBuffer() :
        _size(_BUF_SIZE_PREFERRED),
        _end_offset(0),
        _current_offset(0) {
        _buf = reinterpret_cast<char *>(malloc(_size));
        invariant(_buf);
    }

    RowBuffer::~RowBuffer() {
        free(_buf);
    }

    // append the given key, loc and obj to the end of the buffer
    //
    // important note: empty bson objects still take up 5 bytes. so we
    // always write at least 5 bytes to disk for the key and obj
    void RowBuffer::append(const BSONObj &key, const DiskLoc &loc, const BSONObj &obj) {
        size_t key_size = key.objsize();
        size_t loc_size = sizeof(DiskLoc);
        size_t obj_size = obj.objsize();
        size_t size_needed = _end_offset + key_size + loc_size + obj_size;

        // if we need more than we have, realloc.
        if (size_needed > _size) {
            _buf = reinterpret_cast<char *>(realloc(_buf, size_needed));
            invariant(_buf);
            _size = size_needed;
        }

        // append the key, update the end offset
        memcpy(_buf + _end_offset, key.objdata(), key_size);
        _end_offset += key_size;
        // append the loc, update the end offset
        memcpy(_buf + _end_offset, &loc, loc_size);
        _end_offset += loc_size;
        // append the obj, update the end offset
        memcpy(_buf + _end_offset, obj.objdata(), obj_size);
        _end_offset += obj_size;

        // postcondition: end offset is correctly bounded
        invariant(_end_offset <= _size);
    }

    // the row buffer is gorged if its current size is greater
    // than the preferred size or is almost full.
    // return:
    //      true, buffer is gorged
    //      false, buffer could fit more data
    // rationale:
    //      - if true, then it makes more sense to empty and refill
    //      the buffer than trying to stuff more in it.
    //      - if false, then an append probably won't cause a realloc,
    //      so go ahead and do it.
    bool RowBuffer::isGorged() const {
        const int threshold = 100;
        const bool almost_full = _end_offset + threshold > _size;
        const bool too_big = _size > _BUF_SIZE_PREFERRED;
        return almost_full || too_big;
    }

    // move the internal buffer position to the next key, loc pair
    // returns:
    //      true, the buffer is ready to be read via current()
    //      false, the buffer has no more data
    bool RowBuffer::next() {
        // precondition: there exists a next element advance
        invariant(_current_offset < _end_offset);

        // seek passed the current key, loc, and obj.
        size_t key_size = currentKey().objsize();
        size_t loc_size = sizeof(DiskLoc);
        size_t obj_size = currentObj().objsize();
        _current_offset += key_size + loc_size + obj_size;

        // postcondition: we did not seek passed the end of the buffer
        invariant(_current_offset <= _end_offset);

        // next succeeds if the current offset is still before the end
        return _current_offset < _end_offset ? true : false;
    }

    // get the current key from the buffer. the buffer must have
    // valid data in order to call this, assert otherwise.
    BSONObj RowBuffer::currentKey() const {
        invariant(_current_offset < _end_offset);
        const char *key_buf = _buf + _current_offset;
        BSONObj key = BSONObj(key_buf);
        return key;
    }

    // get the current diskloc from the buffer. the buffer must have
    // valid data in order to call this, assert otherwise.
    DiskLoc RowBuffer::currentLoc() const {
        invariant(_current_offset < _end_offset);
        const char *key_buf = _buf + _current_offset;
        BSONObj key = BSONObj(key_buf);
        DiskLoc *loc = (DiskLoc *) (key_buf + key.objsize());
        return *loc;
    }

    // get the current obj from the buffer.
    BSONObj RowBuffer::currentObj() const {
        invariant(_current_offset < _end_offset);
        const char *key_buf = _buf + _current_offset;
        BSONObj key = BSONObj(key_buf);
        const char *obj_buf = key_buf + key.objsize() + sizeof(DiskLoc);
        BSONObj obj = BSONObj(obj_buf);
        return obj;
    }

    // empty the row buffer, resetting all data and internal positions
    void RowBuffer::empty() {
        free(_buf);
        _buf = reinterpret_cast<char *>(malloc(_BUF_SIZE_PREFERRED));
        _size = _BUF_SIZE_PREFERRED;
        _current_offset = 0;
        _end_offset = 0;
    }
} /* namespace mongo */
