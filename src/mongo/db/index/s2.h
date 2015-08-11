/**
*    Copyright (C) 2012 10gen Inc.
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

#include "mongo/db/index.h"
#include "mongo/db/geo/s2common.h"

namespace mongo {

    class GeoNearArguments;


    // We need cmdObj and parsedArgs so we can print a useful error msg.
    bool run2DSphereGeoNear(const IndexDetails &id, BSONObj& cmdObj,
                            const GeoNearArguments &parsedArgs, string& errmsg,
                            BSONObjBuilder& result);

    class S2Index : public IndexInterface {
    private:
        int configValueWithDefault(const string& name, int def) const {
            BSONElement e = _info[name];
            if (e.isNumber()) { return e.numberInt(); }
            return def;
        }
    public:
        // We keep track of what fields we've indexed and if they're geo or not.
        struct IndexedField {
            enum Type {
                GEO,
                LITERAL
            };

            Type type;
            string name;
            IndexedField(Type t, const string& n) : type(t), name(n) { }
        };

        S2Index(const BSONObj &info);

        ~S2Index();

        // @return the "special" name for this index.
        const string &getSpecialIndexName() const {
            static const string name = "2dsphere";
            return name;
        }

        IndexDetails::Suitability suitability(const FieldRangeSet& queryConstraints,
                                              const BSONObj& order) const;

        // Entry point for a search.
        shared_ptr<Cursor> newCursor(const BSONObj& query, const BSONObj& order,
                                     int numWanted) const;

        // These are used by the geoNear command.  geoNear constructs its own cursor.
        const S2IndexingParams& getParams() const { return _params; }

        void getGeoFieldNames(vector<string> *out) const;

    private:
        S2IndexingParams _params;
        vector<IndexedField> _fields;
    };

}  // namespace mongo
