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
#include "mongo/db/index/2d_descriptor.h"
#include "mongo/db/index/2d_key_generator.h"

namespace mongo {

    TwoDDescriptor::TwoDDescriptor(const BSONObj &keyPattern,
                                   const string &geo,
                                   const vector< pair<string, int> > &other,
                                   const GeoHashConverter::Parameters &params,
                                   const bool sparse,
                                   const bool clustering) :
        Descriptor(keyPattern, Descriptor::TwoD, sparse, clustering),
        _geo(geo),
        _other(other),
        _params(params) {
    }

    TwoDDescriptor::TwoDDescriptor(const char *data, const size_t size) :
        Descriptor(data, size) {
        size_t baseSize = Descriptor::size();
        verify(size == baseSize + sizeof(int) + sizeof(GeoHashConverter::Parameters));

        // Deserialize the geo field, given by an index into the field names array
        int geoFieldIdx;
        memcpy(&geoFieldIdx, data + baseSize, sizeof(int));

        // Deserialize the params
        memcpy(&_params, data + baseSize + sizeof(int), sizeof(GeoHashConverter::Parameters));

        // Fill out _geo and _other based on the field names in the
        // parent Descriptor, plus some ordering info that we need
        // to fill out (for some reason). (2d indexes are dirty..)
        vector<const char *> allFields;
        fieldNames(allFields);
        const Ordering &o(ordering());
        for (vector<const char *>::const_iterator it = allFields.begin();
             it != allFields.end(); it++) {
            const int currIdx = allFields.end() - it;
            if (currIdx == geoFieldIdx) {
                _geo = *it;
            } else {
                const bool ascending = !o.descending(1UL << currIdx);
                _other.push_back(make_pair(string(*it), ascending ? 1 : -1));
            }
        }
        massert(17366, "bug: haystack descriptor did not find the geo field", !_geo.empty());
    }

    DBT TwoDDescriptor::dbt(scoped_array<char> &buf) const {
        scoped_array<char> baseBuf;
        DBT baseDBT = Descriptor::dbt(baseBuf);

        // Rewrite the descriptor but with enough space for 
        // an integer (index into fields array for geo field)
        // and a geo hash converter params struct;
        size_t total_size = baseDBT.size + sizeof(int) + sizeof(GeoHashConverter::Parameters);
        buf.reset(new char[total_size]);
        memcpy(&buf[0], baseDBT.data, baseDBT.size);

        // Serialize the index in the fields array corresponding to the geo field
        vector<const char *> allFields;
        fieldNames(allFields);
        int geoFieldIdx = -1;
        for (vector<const char *>::const_iterator it = allFields.begin();
             it != allFields.end(); it++) {
            if (strcmp(*it, _geo.c_str()) == 0) {
                geoFieldIdx = allFields.end() - it;
            }
        }
        massert(17367, "bug: 2d could not find geoField in Descriptor's field names",
                       geoFieldIdx >= 0);
        memcpy(&buf[baseDBT.size], &geoFieldIdx, sizeof(int));

        // Serialize the parameters
        memcpy(&buf[baseDBT.size + sizeof(int)], &_params, sizeof(GeoHashConverter::Parameters));

        return storage::dbt_make(buf.get(), total_size);
    }

    void TwoDDescriptor::generateKeys(const BSONObj &obj, BSONObjSet &keys) const {
        TwoDKeyGenerator generator(_geo, _other, _params, sparse());
        generator.getKeys(obj, keys);
    }

} // namespace mongo

