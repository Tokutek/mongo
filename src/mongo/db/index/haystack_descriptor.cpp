/** @file haystack_descriptor.cpp */

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
#include "mongo/db/index/haystack_descriptor.h"
#include "mongo/db/index/haystack_key_generator.h"
#include "mongo/db/key_generator.h"
#include "mongo/db/storage/dbt.h"
#include "mongo/db/storage/key.h"
#include "mongo/util/assert_util.h"

namespace mongo {

    HaystackDescriptor::HaystackDescriptor(const BSONObj &keyPattern,
                                           const string &geoField,
                                           const vector<string> &otherFields,
                                           const double bucketSize,
                                           const bool sparse,
                                           const bool clustering) :
        Descriptor(keyPattern, Descriptor::Haystack, sparse, clustering),
        _geoField(geoField),
        _otherFields(otherFields),
        _bucketSize(bucketSize) {
    }

    // A haystack descriptor is a regular one plus:
    // - a double, bucketSize
    // - an integer, the index into the field names array that
    //   corresponds to the geo field

    HaystackDescriptor::HaystackDescriptor(const char *data, const size_t size) :
        Descriptor(data, size) {
        size_t baseSize = Descriptor::size();
        verify(size == baseSize + sizeof(int) + sizeof(double));

        // Deserialize the geo field, given by an index into the field names array
        int geoFieldIdx;
        memcpy(&geoFieldIdx, data + baseSize, sizeof(int));

        // Deserialize the bucket size
        memcpy(&_bucketSize, data + baseSize + sizeof(int), sizeof(double));

        // Fill out geoField and otherFields based on the field names in the
        // parent Descriptor.
        vector<const char *> allFields;
        fieldNames(allFields);
        for (vector<const char *>::const_iterator it = allFields.begin();
             it != allFields.end(); it++) {
            if (allFields.end() - it == geoFieldIdx) {
                _geoField = *it;
            } else {
                _otherFields.push_back(*it);
            }
        }
        massert(17372, "bug: haystack descriptor did not find the geo field", !_geoField.empty());
    }

    DBT HaystackDescriptor::dbt(scoped_array<char> &buf) const {
        scoped_array<char> baseBuf;
        DBT baseDBT = Descriptor::dbt(baseBuf);

        // Rewrite the descriptor but with enough space for
        // a double and an unsigned int.
        size_t total_size = baseDBT.size + sizeof(int) + sizeof(double);
        buf.reset(new char[total_size]);
        memcpy(&buf[0], baseDBT.data, baseDBT.size);

        // Serialize the index in the fields array corresponding to the geo field
        vector<const char *> allFields;
        fieldNames(allFields);
        int geoFieldIdx = -1;
        for (vector<const char *>::const_iterator it = allFields.begin();
             it != allFields.end(); it++) {
            if (strcmp(*it, _geoField.c_str()) == 0) {
                geoFieldIdx = allFields.end() - it;
            }
        }
        massert(17374, "bug: haystack could not find geoField in Descriptor's field names",
                       geoFieldIdx >= 0);
        memcpy(&buf[baseDBT.size], &geoFieldIdx, sizeof(int));

        // Serialize the bucket size
        memcpy(&buf[baseDBT.size + sizeof(int)], &_bucketSize, sizeof(double));

        return storage::dbt_make(buf.get(), total_size);
    }

    void HaystackDescriptor::generateKeys(const BSONObj &obj, BSONObjSet &keys) const {
        HaystackKeyGenerator generator(_geoField, _otherFields, _bucketSize, sparse());
        generator.getKeys(obj, keys);
    }

} // namespace mongo

