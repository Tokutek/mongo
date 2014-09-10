/** @file descriptor.h */

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

#include <string>

#include "mongo/pch.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/descriptor.h"
#include "mongo/db/index/s2_descriptor.h"
#include "mongo/db/index/s2_key_generator.h"
#include "mongo/db/key_generator.h"
#include "mongo/db/storage/dbt.h"
#include "mongo/db/storage/key.h"
#include "mongo/util/assert_util.h"

namespace mongo {

    S2Descriptor::S2Descriptor(const BSONObj &keyPattern,
                               const S2IndexingParams &params,
                               const vector<S2Index::IndexedField> &fields,
                               const bool sparse,
                               const bool clustering) :
        Descriptor(keyPattern, Descriptor::S2, sparse, clustering),
        _params(params), _fields(fields) {
        verify(_fields.size() == Descriptor::numFields());
    }

    // An s2 descriptor is a regular one plus:
    // - S2IndexingParams struct as-is
    // - char array, each byte representing the 'type' for each fieldname
    //   that will be put into the vector of S2Index::IndexedField's

    S2Descriptor::S2Descriptor(const char *data, const size_t size) :
        Descriptor(data, size) {
        size_t baseSize = Descriptor::size();
        verify(size == baseSize +
                       sizeof(S2IndexingParams) +
                       Descriptor::numFields() * sizeof(char));

        // Deserialize the params struct
        memcpy(&_params, data + baseSize, sizeof(S2IndexingParams));

        // Deserialize the array of IndexedField types, creating a
        // corresponding IndexedField object to put into _fields.
        vector<const char *> fields;
        Descriptor::fieldNames(fields);
        const char *typesBase = data + baseSize + sizeof(S2IndexingParams);
        for (size_t i = 0; i < Descriptor::numFields(); i++) {
            S2Index::IndexedField::Type type =
                static_cast<enum S2Index::IndexedField::Type>(typesBase[i]);
            _fields.push_back(S2Index::IndexedField(type, fields[i]));
        }
    }

    DBT S2Descriptor::dbt(scoped_array<char> &buf) const {
        scoped_array<char> baseBuf;
        DBT baseDBT = Descriptor::dbt(baseBuf);

        // Rewrite the descriptor but with enough space for
        // - the s2 indexing params struct
        // - the array of indexedfield types
        size_t total_size = baseDBT.size +
                            sizeof(S2IndexingParams) +
                            Descriptor::numFields() * sizeof(char);
        buf.reset(new char[total_size]);
        memcpy(&buf[0], baseDBT.data, baseDBT.size);

        // Serialize the s2 indexing params struct
        memcpy(&buf[0] + baseDBT.size, &_params, sizeof(S2IndexingParams));

        // Serialize the array of IndexedField types
        char *typesBase = &buf[0] + baseDBT.size + sizeof(S2IndexingParams);
        for (size_t i = 0; i < Descriptor::numFields(); i++) {
            typesBase[i] = _fields[i].type;
        }

        return storage::dbt_make(buf.get(), total_size);
    }

    void S2Descriptor::generateKeys(const BSONObj &obj, BSONObjSet &keys) const {
        S2KeyGenerator generator(_fields, _params, sparse());
        generator.getKeys(obj, keys);
    }

} // namespace mongo

