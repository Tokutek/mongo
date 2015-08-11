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
#include "mongo/db/index/haystack.h"

namespace mongo {

    class HaystackKeyGenerator : public KeyGenerator {
    public:
        HaystackKeyGenerator(const string &geoField,
                             const vector<string> &otherFields,
                             const double bucketSize,
                             const bool sparse);
        virtual ~HaystackKeyGenerator() { }

        virtual void getKeys(const BSONObj &obj, BSONObjSet &keys) const;

    private:
        string _geoField;
        vector<string> _otherFields;
        double _bucketSize;
    };

} // namespace mongo
