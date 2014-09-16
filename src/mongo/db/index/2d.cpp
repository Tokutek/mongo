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

#include "pch.h"

#include <vector>

#include "mongo/db/jsobj.h"
#include "mongo/db/index.h"
#include "mongo/db/index/2d.h"
#include "mongo/db/index/2d_descriptor.h"
#include "mongo/db/index/2d_key_generator.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/geo/2d.h"
#include "mongo/db/geo/hash.h"

namespace mongo {

    static const string GEO2DNAME = "2d";

    TwoDIndex::TwoDIndex(const BSONObj &info) : IndexInterface(info) {
        BSONObjIterator i(_keyPattern);
        while (i.more()) {
            BSONElement e = i.next();
            if (e.type() == String && GEO2DNAME == e.valuestr()) {
                uassert(13022, "can't have 2 geo field", _geo.size() == 0);
                uassert(13023, "2d has to be first in index", _other.size() == 0);
                _geo = e.fieldName();
            } else {
                int order = 1;
                if (e.isNumber()) {
                    order = static_cast<int>(e.Number());
                }
                _other.push_back(make_pair(e.fieldName(), order));
            }
        }
        uassert(13024, "no geo field specified", _geo.size());

        double bits = configValueWithDefault("bits", 26); // for lat/long, ~ 1ft
        uassert(13028, "bits in geo index must be between 1 and 32", bits > 0 && bits <= 32);

        GeoHashConverter::Parameters params;
        memset(&params, 0, sizeof(GeoHashConverter::Parameters));
        params.bits = static_cast<unsigned>(bits);
        params.max = configValueWithDefault("max", 180.0);
        params.min = configValueWithDefault("min", -180.0);
        double numBuckets = (1024 * 1024 * 1024 * 4.0);
        params.scaling = numBuckets / (params.max - params.min);

        _geoHashConverter.reset(new GeoHashConverter(params));

        // Create a 2d descriptor that will store params in the dicitonary's descriptor
        _descriptor.reset(new TwoDDescriptor(_keyPattern, _geo, _other, params, _sparse, _clustering));

        // Reset the key generator to be an 2d key generator
        vector<const char *> fieldNames;
        _descriptor->fieldNames(fieldNames);
        _keyGenerator.reset(new TwoDKeyGenerator(_geo, _other, params, _sparse));
    }

    BSONObj TwoDIndex::fixKey(const BSONObj& in) const {
        if (in.firstElement().type() == BinData)
            return in;

        BSONObjBuilder b(in.objsize() + 16);

        if (in.firstElement().isABSONObj())
            _geoHashConverter->hash(in.firstElement().embeddedObject()).appendToBuilder(&b, "");
        else if (in.firstElement().type() == String)
            GeoHash(in.firstElement().valuestr()).appendToBuilder(&b, "");
        else if (in.firstElement().type() == RegEx)
            GeoHash(in.firstElement().regex()).appendToBuilder(&b, "");
        else
            return in;

        BSONObjIterator i(in);
        i.next();
        while (i.more())
            b.append(i.next());
        return b.obj();
    }

    shared_ptr<mongo::Cursor> TwoDIndex::newCursor(const BSONObj& query, const BSONObj& order,
                                                   int numWanted) const {
        // Implemented in geo/2d.cpp to keep this file free of a ton of geo code
        return new2DCursor(this, _geo, query, order, numWanted);
    }

    IndexDetails::Suitability TwoDIndex::suitability( const FieldRangeSet& queryConstraints ,
                                                      const BSONObj& order ) const {
        BSONObj query = queryConstraints.originalQuery();

        BSONElement e = query.getFieldDotted(_geo.c_str());
        switch (e.type()) {
        case Object: {
            BSONObj sub = e.embeddedObject();
            switch (sub.firstElement().getGtLtOp()) {
            case BSONObj::opNEAR:
                return OPTIMAL;
            case BSONObj::opWITHIN: {
                // Don't return optimal if it's $within: {$geometry: ... }
                // because we will error out in that case, but the matcher
                // or 2dsphere index may handle it.
                BSONElement elt = sub.firstElement();
                if (Object == elt.type()) {
                    BSONObjIterator it(elt.embeddedObject());
                    while (it.more()) {
                        BSONElement elt = it.next();
                        if (mongoutils::str::equals("$geometry", elt.fieldName())) {
                            return USELESS;
                        }
                    }
                }
                return OPTIMAL;
            }
            default:
                // We can try to match if there's no other indexing defined,
                // this is assumed a point
                return HELPFUL;
            }
        }
        case Array:
            // We can try to match if there's no other indexing defined,
            // this is assumed a point
            return HELPFUL;
        default:
            return USELESS;
        }
    }

} // namespace mongo
