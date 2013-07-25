/** @file descriptor.cpp */

/**
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
#include "mongo/db/descriptor.h"
#include "mongo/db/indexkey.h"

namespace mongo {

    Descriptor::Descriptor(const BSONObj &keyPattern,
                           const bool hashed,
                           const int hashSeed,
                           const bool sparse,
                           const bool clustering) :
        _data(NULL), _size(serializedSize(keyPattern)), _dataOwned(new char[_size]) {
        _data = _dataOwned.get();

        // Create a header and write it first.
        Header h(Ordering::make(keyPattern),
                 hashed, hashSeed, sparse, clustering, keyPattern.nFields());
        memcpy(_dataOwned.get(), &h, sizeof(Header));

        // The offsets array is based after the header. It is an array of
        // size h.numFields, where each element is sizeof(uint32_t) bytes.
        // The fields array is based after the offsets array.
        uint32_t *const offsetsBase = reinterpret_cast<uint32_t *>(_dataOwned.get() + sizeof(Header));
        char *const fieldsBase = reinterpret_cast<char *>(offsetsBase + h.numFields);

        // Write each field's offset and value into each array, respectively.
        int i = 0, offset = 0;
        for (BSONObjIterator o(keyPattern); o.more(); ++o, ++i) {
            const BSONElement &e = *o;
            const char *field = e.fieldName();
            const size_t len = strlen(field) + 1;
            memcpy(fieldsBase + offset, field, len);
            offsetsBase[i] = offset;
            offset += len;
            verify((char*) &offsetsBase[i] < fieldsBase);
        }
        verify(fieldsBase + offset == _data + _size);
    }

    Descriptor::Descriptor(const char *data, const size_t size) :
        _data(data), _size(size) {
        verify(_data != NULL);
        const Header &h(*reinterpret_cast<const Header *>(_data));
        h.checkVersion();

        // Strictly greater, since there should be at least one field.
        verify(_size > FixedSize);
    }

    size_t Descriptor::serializedSize(const BSONObj &keyPattern) {
        size_t size = FixedSize;
        for (BSONObjIterator o(keyPattern); o.more(); ++o) {
            const BSONElement &e = *o;
            // Each field will take up 4 bytes in the offset array
            // and the size of the field name (and null terminator).
            size += 4;
            size += strlen(e.fieldName()) + 1;
        }
        verify(size > FixedSize);
        return size;
    }

    void Descriptor::assertEqual(const Descriptor &rhs) const {
        const bool equal = _size == rhs._size && memcmp(_data, rhs._data, _size) == 0;
        verify(equal);
    }

    DBT Descriptor::dbt() const {
        return storage::make_dbt(_data, _size);
    }

    int Descriptor::compareKeys(const storage::Key &key1, const storage::Key &key2) const {
        const Header &h(*reinterpret_cast<const Header *>(_data));
        return key1.woCompare(key2, h.ordering);
    }

    void Descriptor::generateKeys(const BSONObj &obj, BSONObjSet &keys) const {
        const Header &h(*reinterpret_cast<const Header *>(_data));
        const uint32_t *const offsetsBase = reinterpret_cast<const uint32_t *>(_data + sizeof(Header));
        const char *const fieldsBase = reinterpret_cast<const char *>(offsetsBase + h.numFields);
        vector<const char *> fieldNames(h.numFields);
        for (uint32_t i = 0; i < h.numFields; i++) {
            fieldNames[i] = fieldsBase + offsetsBase[i];
        }
        if (h.hashed) {
            KeyGenerator::getHashedKey(obj, fieldNames[0], h.hashSeed, h.sparse, keys);
        } else {
            KeyGenerator::getKeys(obj, fieldNames, h.sparse, keys);
        }
    }

} // namespace mongo
