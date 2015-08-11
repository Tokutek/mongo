// keygenerator.h

/**
*    Copyright (C) 2008 10gen Inc.
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

#pragma once

#include <vector>

#include "mongo/pch.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/index/s2.h"

namespace mongo {

    class S2KeyGenerator : public KeyGenerator {
    public:
        S2KeyGenerator(const vector<S2Index::IndexedField> fieldNames,
                       const S2IndexingParams &params,
                       const bool sparse);
        virtual ~S2KeyGenerator() { }

        virtual void getKeys(const BSONObj &obj, BSONObjSet &keys) const;

    private:
        // Get the index keys for elements that are GeoJSON.
        void getGeoKeys(const BSONElementSet &elements, BSONObjSet *out) const;

        void getLiteralKeysArray(BSONObj obj, BSONObjSet *out) const;

        void getOneLiteralKey(BSONElement elt, BSONObjSet *out) const;

        // elements is a non-geo field.  Add the values literally, expanding arrays.
        void getLiteralKeys(const BSONElementSet &elements, BSONObjSet *out) const;

        const S2IndexingParams _params;
        const vector<S2Index::IndexedField> _fields;
    };

} // namespace mongo
