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

#include <vector>

#include "mongo/pch.h"
#include "mongo/db/index/s2_key_generator.h"
#include "mongo/db/geo/s2.h"
#include "third_party/s2/s2cell.h"
#include "third_party/s2/s2polygon.h"
#include "third_party/s2/s2polyline.h"
#include "third_party/s2/s2regioncoverer.h"

namespace mongo {

    S2KeyGenerator::S2KeyGenerator(const vector<S2Index::IndexedField> fieldNames,
                                   const S2IndexingParams &params,
                                   const bool sparse) :
        // Just pass an empty vector of field names to the base class - they're unused
        KeyGenerator(vector<const char *>(), sparse),
        _params(params), _fields(fieldNames) {
    }

    void S2KeyGenerator::getKeys(const BSONObj &obj, BSONObjSet &keys) const {
        verify(_fields.size() >= 1);

        BSONObjSet keysToAdd;
        // We output keys in the same order as the fields we index.
        for (size_t i = 0; i < _fields.size(); ++i) {
            const S2Index::IndexedField &field = _fields[i];

            // First, we get the keys that this field adds.  Either they're added literally from
            // the value of the field, or they're transformed if the field is geo.
            BSONElementSet fieldElements;
            // false means Don't expand the last array, duh.
            obj.getFieldsDotted(field.name, fieldElements, false);

            BSONObjSet keysForThisField;
            if (S2Index::IndexedField::GEO == field.type) {
                getGeoKeys(fieldElements, &keysForThisField);
            } else if (S2Index::IndexedField::LITERAL == field.type) {
                getLiteralKeys(fieldElements, &keysForThisField);
            } else {
                verify(0);
            }

            // We expect there to be _index->_missingField() present in the keys if data is
            // missing.  So, this should be non-empty.
            verify(!keysForThisField.empty());

            // We take the Cartesian product of all of the keys.  This requires that we have
            // some keys to take the Cartesian product with.  If keysToAdd.empty(), we
            // initialize it.  
            if (keysToAdd.empty()) {
                keysToAdd = keysForThisField;
                continue;
            }

            BSONObjSet updatedKeysToAdd;
            for (BSONObjSet::const_iterator it = keysToAdd.begin(); it != keysToAdd.end();
                 ++it) {
                for (BSONObjSet::const_iterator newIt = keysForThisField.begin();
                     newIt!= keysForThisField.end(); ++newIt) {
                    BSONObjBuilder b;
                    b.appendElements(*it);
                    b.append(newIt->firstElement());
                    updatedKeysToAdd.insert(b.obj());
                }
            }
            keysToAdd = updatedKeysToAdd;
        }

        if (keysToAdd.size() > _params.maxKeysPerInsert) {
            warning() << "insert of geo object generated lots of keys (" << keysToAdd.size()
                << ") consider creating larger buckets. obj="
                << obj;
        }

        for (BSONObjSet::const_iterator it = keysToAdd.begin(); it != keysToAdd.end(); ++it) {
            keys.insert(*it);
        }
    }

    static void keysFromRegion(S2RegionCoverer *coverer, const S2Region &region,
                               vector<string> *out) {
        vector<S2CellId> covering;
        coverer->GetCovering(region, &covering);
        for (size_t i = 0; i < covering.size(); ++i) {
            out->push_back(covering[i].toString());
        }
    }

    // Get the index keys for elements that are GeoJSON.
    void S2KeyGenerator::getGeoKeys(const BSONElementSet &elements, BSONObjSet *out) const {
        S2RegionCoverer coverer;
        _params.configureCoverer(&coverer);

        // See here for GeoJSON format: geojson.org/geojson-spec.html
        for (BSONElementSet::iterator i = elements.begin(); i != elements.end(); ++i) {
            uassert(16700, "Can't parse geometry from element: " + i->toString(),
                    i->isABSONObj());
            const BSONObj &obj = i->Obj();

            vector<string> cells;
            S2Polyline line;
            S2Cell point;
            // We only support GeoJSON polygons.  Why?:
            // 1. we don't automagically do WGS84/flat -> WGS84, and
            // 2. the old polygon format must die.
            if (GeoParser::isGeoJSONPolygon(obj)) {
                S2Polygon polygon;
                GeoParser::parseGeoJSONPolygon(obj, &polygon);
                keysFromRegion(&coverer, polygon, &cells);
            } else if (GeoParser::parseLineString(obj, &line)) {
                keysFromRegion(&coverer, line, &cells);
            } else if (GeoParser::parsePoint(obj, &point)) {
                S2CellId parent(point.id().parent(_params.finestIndexedLevel));
                cells.push_back(parent.toString());
            } else {
                uasserted(16572, "Can't extract geo keys from object, malformed geometry?:"
                                 + obj.toString());
            }
            uassert(16673, "Unable to generate keys for (likely malformed) geometry: "
                           + obj.toString(),
                    cells.size() > 0);

            for (vector<string>::const_iterator it = cells.begin(); it != cells.end(); ++it) {
                BSONObjBuilder b;
                b.append("", *it);
                out->insert(b.obj());
            }
        }

        if (0 == out->size()) {
            BSONObjBuilder b;
            b.appendNull("");
            out->insert(b.obj());
        }
    }

    void S2KeyGenerator::getLiteralKeysArray(BSONObj obj, BSONObjSet *out) const {
        BSONObjIterator objIt(obj);
        if (!objIt.more()) {
            // Empty arrays are indexed as undefined.
            BSONObjBuilder b;
            b.appendUndefined("");
            out->insert(b.obj());
        } else {
            // Non-empty arrays are exploded.
            while (objIt.more()) {
                BSONObjBuilder b;
                b.appendAs(objIt.next(), "");
                out->insert(b.obj());
            }
        }
    }

    void S2KeyGenerator::getOneLiteralKey(BSONElement elt, BSONObjSet *out) const {
        if (Array == elt.type()) {
            getLiteralKeysArray(elt.Obj(), out);
        } else {
            // One thing, not an array, index as-is.
            BSONObjBuilder b;
            b.appendAs(elt, "");
            out->insert(b.obj());
        }
    }

    // elements is a non-geo field.  Add the values literally, expanding arrays.
    void S2KeyGenerator::getLiteralKeys(const BSONElementSet &elements, BSONObjSet *out) const {
        if (0 == elements.size()) {
            // Missing fields are indexed as null.
            BSONObjBuilder b;
            b.appendNull("");
            out->insert(b.obj());
        } else {
            for (BSONElementSet::iterator i = elements.begin(); i != elements.end(); ++i) {
                getOneLiteralKey(*i, out);
            }
        }
    }

} // namespace mongo
