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
#include "mongo/db/index/hashed.h"
#include "mongo/db/index/hashed_descriptor.h"
#include "mongo/db/index/hashed_key_generator.h"

namespace mongo {

    // For interpretting a memory buffer as a descriptor.
    HashedDescriptor::HashedDescriptor(const char *data, const size_t size) :
        Descriptor(data, size) {
        if (version() <= Descriptor::VERSION_1) {
            // Legacy hashed descriptor's stored the hash seed in the header
            // and the hash version was fixed at 0.
            _hashSeed = _hashSeedDeprecated();
            _hashVersion = 0;
        } else {
            memcpy(&_hashSeed, data + Descriptor::size(), sizeof(int));
            memcpy(&_hashVersion, data + Descriptor::size() + sizeof(int), sizeof(int)); 
            // A hashed descriptor is a regular one plus two ints.
            verify(size == Descriptor::size() + sizeof(int) + sizeof(int));
        }
    }

    DBT HashedDescriptor::dbt(scoped_array<char> &buf) const {
        scoped_array<char> baseBuf;
        DBT baseDBT = Descriptor::dbt(baseBuf);

        // Rewrite the descriptor but with enough space for two more ints.
        size_t total_size = baseDBT.size + sizeof(int) + sizeof(int);
        buf.reset(new char[total_size]);
        memcpy(&buf[0], baseDBT.data, baseDBT.size);

        // Add on the hashed descriptor specific info
        memcpy(&buf[baseDBT.size], &_hashSeed, sizeof(int));
        memcpy(&buf[baseDBT.size + sizeof(int)], &_hashVersion, sizeof(int));
        return storage::dbt_make(buf.get(), total_size);
    }

    void HashedDescriptor::generateKeys(const BSONObj &obj, BSONObjSet &keys) const {
        vector<const char *> fields;
        fieldNames(fields);
        HashedKeyGenerator generator(fields,  _hashSeed, _hashVersion, sparse());
        generator.getKeys(obj, keys);
    }

} // namespace mongo

