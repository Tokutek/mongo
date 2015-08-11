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
#include "mongo/db/index/2d.h"

namespace mongo {

    class TwoDKeyGenerator : public KeyGenerator {
    public:
        TwoDKeyGenerator(const string &geo,
                         const vector< pair<string, int> > &other,
                         const GeoHashConverter::Parameters &params,
                         const bool sparse);
        virtual ~TwoDKeyGenerator() { }

        void getKeys(const BSONObj &obj, BSONObjSet &keys) const;

        void getGeoLocs(const BSONObj &obj, vector<BSONObj> &locs) const;

    private:
        virtual void _getKeys(const BSONObj &obj, BSONObjSet *keys, vector<BSONObj> *locs) const;
        string _geo;
        vector<pair<string, int> > _other;
        scoped_ptr<GeoHashConverter> _geoHashConverter;
    };

} // namespace mongo
