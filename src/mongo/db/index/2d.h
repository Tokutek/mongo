/**
*    Copyright (C) 2008 10gen Inc.
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

#include "pch.h"

#include <vector>

#include "mongo/db/jsobj.h"
#include "mongo/db/index.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/geo/hash.h"

namespace mongo {

    class TwoDIndex : public IndexInterface {
    public:
        TwoDIndex(const BSONObj &info);

        virtual ~TwoDIndex() { }

        // @return the "special" name for this index.
        const string &getSpecialIndexName() const {
            static const string name = "2d";
            return name;
        }

        BSONObj fixKey(const BSONObj& in) const;

        shared_ptr<Cursor> newCursor(const BSONObj& query, const BSONObj& order,
                                     int numWanted) const;

        IndexDetails::Suitability suitability( const FieldRangeSet& queryConstraints ,
                                               const BSONObj& order ) const;

        const GeoHashConverter& getConverter() const { return *_geoHashConverter; }

        // XXX: make private with a getter
        string _geo;
        vector<pair<string, int> > _other;
    private:
        double configValueWithDefault(const string& name, double def) {
            BSONElement e = _info[name];
            if (e.isNumber()) {
                return e.numberDouble();
            }
            return def;
        }

        scoped_ptr<GeoHashConverter> _geoHashConverter;
    };

} // namespace mongo
