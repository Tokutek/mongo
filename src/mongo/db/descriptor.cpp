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
#include "mongo/db/keygenerator.h"

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
                 hashed, sparse, clustering, hashSeed, keyPattern.nFields());
        memcpy(_dataOwned.get(), &h, sizeof(Header));

        // The offsets array is based after the header. It is an array of
        // size h.numFields, where each element is sizeof(uint32_t) bytes.
        // The fields array is based after the offsets array.
        uint32_t *const offsetsBase = reinterpret_cast<uint32_t *>(_dataOwned.get() + sizeof(Header));
        char *const fieldsBase = reinterpret_cast<char *>(&offsetsBase[h.numFields]);

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
        // Strictly greater, since there should be at least one field.
        verify(_size > (size_t) FixedSize);
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
        verify(size > (size_t) FixedSize);
        return size;
    }

    bool Descriptor::operator==(const Descriptor &rhs) const {
        return _size == rhs._size && memcmp(_data, rhs._data, _size) == 0;
    }

    int Descriptor::version() const {
        const Header &h(*reinterpret_cast<const Header *>(_data));
        return h.version;
    }

    const Ordering &Descriptor::ordering() const {
        const Header &h(*reinterpret_cast<const Header *>(_data));
        return h.ordering;
    }

    void Descriptor::fieldNames(vector<const char *> &fields) const {
        const Header &h(*reinterpret_cast<const Header *>(_data));
        const uint32_t *const offsetsBase = reinterpret_cast<const uint32_t *>(_data + sizeof(Header));
        const char *const fieldsBase = reinterpret_cast<const char *>(offsetsBase + h.numFields);
        fields.resize(h.numFields);
        for (uint32_t i = 0; i < h.numFields; i++) {
            fields[i] = fieldsBase + offsetsBase[i];
        }
    }

    BSONObj Descriptor::fillKeyFieldNames(const BSONObj &key) const {
        BSONObjBuilder b;
        vector<const char *> fields;
        fieldNames(fields);
        BSONObjIterator o(key);
        for (vector<const char *>::const_iterator i = fields.begin();
             i != fields.end(); i++) {
            b.appendAs(o.next(), *i);
        }
        return b.obj();
    }

    DBT Descriptor::dbt() const {
        return storage::dbt_make(_data, _size);
    }

    void Descriptor::generateKeys(const BSONObj &obj, BSONObjSet &keys) const {
        const Header &h(*reinterpret_cast<const Header *>(_data));
        vector<const char *> fields;
        fieldNames(fields);
        if (h.hashed) {
            // If we ever add new hash versions in the future, we'll need to add
            // a hashVersion field to the descriptor and up the descriptor version.
            const int hashVersion = 0;
            HashKeyGenerator generator(fields[0], h.hashSeed, hashVersion, h.sparse);
            generator.getKeys(obj, keys);
        } else {
            KeyGenerator::getKeys(obj, fields, h.sparse, keys);
        }
    }

} // namespace mongo
