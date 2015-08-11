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
#include "mongo/db/index/2d_descriptor.h"
#include "mongo/db/index/hashed_descriptor.h"
#include "mongo/db/index/haystack_descriptor.h"
#include "mongo/db/index/s2_descriptor.h"
#include "mongo/db/key_generator.h"

namespace mongo {

    Descriptor::Descriptor(const BSONObj &keyPattern,
                           const Type type,
                           const bool sparse,
                           const bool clustering) :
        _data(NULL), _size(serializedSizeCurrentVersion(keyPattern)), _dataOwned(new char[_size]) {
        _data = _dataOwned.get();

        // Create a header and write it first.
        verify(static_cast<unsigned int>(type) < 255);
        const uint32_t nFields = keyPattern.nFields();
        verify(nFields > 0);

        // The field names have size equal to total size, minus the header,
        // minus the size of the array of offsets into the field names.
        const int32_t fieldsLength = _size - sizeof(Header) - (4 * nFields);
        verify(fieldsLength > 0);

        Header h(Ordering::make(keyPattern), type, sparse, clustering,
                 nFields, static_cast<uint32_t>(fieldsLength));
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

    void Descriptor::getFromDBT(const DBT *dbt, scoped_ptr<Descriptor> &ptr) {
        const char *data = reinterpret_cast<char *>(dbt->data);
        size_t size = dbt->size;

        Descriptor desc(data, size);
        switch (desc.type()) {
        case Basic:
            ptr.reset(new Descriptor(data, size));
            break;
        case Hashed:
            ptr.reset(new HashedDescriptor(data, size));
            break;
        case S2:
            ptr.reset(new S2Descriptor(data, size));
            break;
        case Haystack:
            ptr.reset(new HaystackDescriptor(data, size));
            break;
        case TwoD:
            ptr.reset(new TwoDDescriptor(data, size));
            break;
        default:
            abort();
        }
    }

    size_t Descriptor::serializedSizeCurrentVersion(const BSONObj &keyPattern) {
        size_t size = sizeof(Header);
        for (BSONObjIterator o(keyPattern); o.more(); ++o) {
            const BSONElement &e = *o;
            // Each field will take up 4 bytes in the offset array
            // and the size of the field name (and null terminator).
            size += 4;
            size += strlen(e.fieldName()) + 1;
        }
        verify(size > sizeof(Header));
        return size;
    }

    void Descriptor::fieldNames(vector<const char *> &fields) const {
        // We use _headerSize() over sizeof(Header) since the header may be <= v1
        const Header &h(*reinterpret_cast<const Header *>(_data));
        const uint32_t *const offsetsBase = reinterpret_cast<const uint32_t *>(_data + _headerSize());
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

    DBT Descriptor::dbt(scoped_array<char> &buf) const {
        buf.reset(new char[_size]);
        memcpy(&buf[0], _data, _size);
        return storage::dbt_make(buf.get(), _size);
    }

    void Descriptor::generateKeys(const BSONObj &obj, BSONObjSet &keys) const {
        const Header &h(*reinterpret_cast<const Header *>(_data));
        vector<const char *> fields;
        fieldNames(fields);

        KeyGenerator generator(fields, h.sparse);
        generator.getKeys(obj, keys);
    }

} // namespace mongo
