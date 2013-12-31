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

#include "mongo/db/client.h"
#include "mongo/db/curop.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/index.h"
#include "mongo/db/index/s2.h"
#include "mongo/db/index/s2_descriptor.h"
#include "mongo/db/index/s2_key_generator.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/geo/geonear.h"
#include "mongo/db/geo/geoparser.h"
#include "mongo/db/geo/geoquery.h"
#include "mongo/db/geo/s2.h"
#include "mongo/db/geo/s2common.h"
#include "mongo/db/geo/s2cursor.h"
#include "mongo/db/geo/s2nearcursor.h"
#include "third_party/s2/s2cell.h"
#include "third_party/s2/s2polygon.h"
#include "third_party/s2/s2polyline.h"
#include "third_party/s2/s2regioncoverer.h"

namespace mongo {

    S2Index::S2Index(const BSONObj &info) : IndexInterface(info) {
        memset(&_params, 0, sizeof(S2IndexingParams));

        _params.maxKeysPerInsert = 200;
        // This is advisory.
        _params.maxCellsInCovering = 50;
        // Near distances are specified in meters...sometimes.
        _params.radius = S2IndexingParams::kRadiusOfEarthInMeters;
        // These are not advisory.
        _params.finestIndexedLevel = configValueWithDefault("finestIndexedLevel",
            S2::kAvgEdge.GetClosestLevel(500.0 / _params.radius));
        _params.coarsestIndexedLevel = configValueWithDefault("coarsestIndexedLevel",
            S2::kAvgEdge.GetClosestLevel(100 * 1000.0 / _params.radius));
        uassert(16687, "coarsestIndexedLevel must be >= 0", _params.coarsestIndexedLevel >= 0);
        uassert(16688, "finestIndexedLevel must be <= 30", _params.finestIndexedLevel <= 30);
        uassert(16689, "finestIndexedLevel must be >= coarsestIndexedLevel",
                _params.finestIndexedLevel >= _params.coarsestIndexedLevel);
        const BSONElement &indexVersionElt = info["2dsphereIndexVersion"];
        massert(17369,
                str::stream() << "unsupported geo index version { "
                              << indexVersionElt
                              << " }, only support versions: [1]",
                indexVersionElt.eoo() || indexVersionElt.numberInt() == 1);

        int geoFields = 0;
        // Categorize the fields we're indexing and make sure we have a geo field.
        BSONObjIterator i(_keyPattern);
        while (i.more()) {
            BSONElement e = i.next();
            if (e.type() == String && string("2dsphere") == e.valuestr()) {
                _fields.push_back(IndexedField(IndexedField::GEO, e.fieldName()));
                ++geoFields;
            } else {
                _fields.push_back(IndexedField(IndexedField::LITERAL, e.fieldName()));
            }
        }
        uassert(16450, "Expect at least one geo field, keyPattern=" + _keyPattern.toString(),
                geoFields >= 1);

        // Create an s2 descriptor that will store _parmas in the dicitonary's descriptor
        _descriptor.reset(new S2Descriptor(_keyPattern, _params, _fields, _sparse, _clustering));

        // Reset the key generator to be an s2 key generator
        vector<const char *> fieldNames;
        _descriptor->fieldNames(fieldNames);
        _keyGenerator.reset(new S2KeyGenerator(_fields, _params, _sparse));
    }

    S2Index::~S2Index() { }

    // Entry point for a search.
    shared_ptr<Cursor> S2Index::newCursor(const BSONObj& query, const BSONObj& order,
                                          int numWanted) const {
        vector<GeoQuery> regions;
        bool isNearQuery = false;
        NearQuery nearQuery;

        // Go through the fields that we index, and for each geo one, make
        // a GeoQuery object for the S2*Cursor class to do intersection
        // testing/cover generating with.
        for (size_t i = 0; i < _fields.size(); ++i) {
            const IndexedField &field = _fields[i];
            if (IndexedField::GEO != field.type) { continue; }

            BSONElement e = query.getFieldDotted(field.name);
            if (e.eoo()) { continue; }
            if (!e.isABSONObj()) { continue; }
            BSONObj obj = e.Obj();

            if (nearQuery.parseFrom(obj, _params.radius)) {
                uassert(16685, "Only one $near clause allowed: " + query.toString(),
                        !isNearQuery);
                isNearQuery = true;
                nearQuery.field = field.name;
                continue;
            }

            GeoQuery geoQueryField(field.name);
            if (!geoQueryField.parseFrom(obj)) {
                uasserted(16535, "can't parse query (2dsphere): " + obj.toString());
            }
            uassert(16684, "Geometry unsupported: " + obj.toString(),
                    geoQueryField.hasS2Region());
            regions.push_back(geoQueryField);
        }

        // Remove all the indexed geo regions from the query.  The s2*cursor will
        // instead create a covering for that key to speed up the search.
        //
        // One thing to note is that we create coverings for indexed geo keys during
        // a near search to speed it up further.
        BSONObjBuilder geoFieldsToNuke;
        if (isNearQuery) {
            geoFieldsToNuke.append(nearQuery.field, "");
        }
        for (size_t i = 0; i < regions.size(); ++i) {
            geoFieldsToNuke.append(regions[i].getField(), "");
        }

        // false means we want to filter OUT geoFieldsToNuke, not filter to include only that.
        BSONObj filteredQuery = query.filterFieldsUndotted(geoFieldsToNuke.obj(), false);

        if (isNearQuery) {
            S2NearCursor *cursor = new S2NearCursor(keyPattern(), this, filteredQuery,
                nearQuery, regions, _params);
            return shared_ptr<Cursor>(cursor);
        } else {
            S2Cursor *cursor = new S2Cursor(keyPattern(), this, filteredQuery, regions, 
                                            _params);
            return shared_ptr<Cursor>(cursor);
        }
    }

    IndexDetails::Suitability S2Index::suitability(const FieldRangeSet& queryConstraints,
                                                   const BSONObj& order) const {
        BSONObj query = queryConstraints.originalQuery();

        for (size_t i = 0; i < _fields.size(); ++i) {
            const IndexedField &field = _fields[i];
            if (IndexedField::GEO != field.type) { continue; }

            BSONElement e = query.getFieldDotted(field.name);
            // Some locations are given to us as arrays.  Sigh.
            if (Array == e.type()) { return HELPFUL; }
            if (Object != e.type()) { continue; }
            // getGtLtOp is horribly misnamed and really means get the operation.
            switch (e.embeddedObject().firstElement().getGtLtOp()) {
                case BSONObj::opNEAR:
                    return OPTIMAL;
                case BSONObj::opWITHIN: {
                    BSONElement elt = e.embeddedObject().firstElement();
                    if (Object != elt.type()) { continue; }
                    const char* fname = elt.embeddedObject().firstElement().fieldName();
                    if (mongoutils::str::equals("$geometry", fname)
                        || mongoutils::str::equals("$centerSphere", fname)) {
                        return OPTIMAL;
                    } else {
                        return USELESS;
                    }
                }
                case BSONObj::opGEO_INTERSECTS:
                    return OPTIMAL;
                default:
                    return USELESS;
            }
        }
        return USELESS;
    }

    void S2Index::getGeoFieldNames(vector<string> *out) const {
        for (size_t i = 0; i < _fields.size(); ++i) {
            if (IndexedField::GEO == _fields[i].type) {
                out->push_back(_fields[i].name);
            }
        }
    }

    // Command to run a 2dsphere query - probably belongs in another file
    // --------------------------------------------------------------------- //

    bool run2DSphereGeoNear(const IndexDetails &id, BSONObj& cmdObj,
                            const GeoNearArguments &parsedArgs, string& errmsg,
                            BSONObjBuilder& result) {
        S2Index *idxType = dynamic_cast<S2Index *>(const_cast<IndexDetails *>(&id));

        vector<string> geoFieldNames;
        idxType->getGeoFieldNames(&geoFieldNames);

        // NOTE(hk): If we add a new argument to geoNear, we could have a
        // 2dsphere index with multiple indexed geo fields, and the geoNear
        // could pick the one to run over.  Right now, we just require one.
        uassert(16552, "geoNear requires exactly one indexed geo field", 1 == geoFieldNames.size());
        NearQuery nearQuery(geoFieldNames[0]);
        uassert(16679, "Invalid geometry given as arguments to geoNear: " + cmdObj.toString(),
                nearQuery.parseFromGeoNear(cmdObj, idxType->getParams().radius));
        uassert(16683, "geoNear on 2dsphere index requires spherical",
                parsedArgs.isSpherical);

        // NOTE(hk): For a speedup, we could look through the query to see if
        // we've geo-indexed any of the fields in it.
        vector<GeoQuery> regions;

        scoped_ptr<S2NearCursor> cursor(new S2NearCursor(idxType->keyPattern(),
            &id, parsedArgs.query, nearQuery, regions, idxType->getParams()));

        double totalDistance = 0;
        BSONObjBuilder resultBuilder(result.subarrayStart("results"));
        double farthestDist = 0;

        int results;
        for (results = 0; results < parsedArgs.numWanted && cursor->ok(); ++results) {
            double dist = cursor->currentDistance();
            // If we got the distance in radians, output it in radians too.
            if (nearQuery.fromRadians) { dist /= idxType->getParams().radius; }
            dist *= parsedArgs.distanceMultiplier;
            totalDistance += dist;
            if (dist > farthestDist) { farthestDist = dist; }

            BSONObjBuilder oneResultBuilder(
                resultBuilder.subobjStart(BSONObjBuilder::numStr(results)));
            oneResultBuilder.append("dis", dist);
            if (parsedArgs.includeLocs) {
                BSONElementSet geoFieldElements;
                cursor->current().getFieldsDotted(geoFieldNames[0], geoFieldElements, false);
                for (BSONElementSet::iterator oi = geoFieldElements.begin();
                        oi != geoFieldElements.end(); ++oi) {
                    if (oi->isABSONObj()) {
                        oneResultBuilder.appendAs(*oi, "loc");
                    }
                }
            }

            oneResultBuilder.append("obj", cursor->current());
            oneResultBuilder.done();
            cursor->advance();
        }

        resultBuilder.done();

        BSONObjBuilder stats(result.subobjStart("stats"));
        stats.append("time", cc().curop()->elapsedMillis());
        stats.appendNumber("nscanned", cursor->nscanned());
        stats.append("avgDistance", totalDistance / results);
        stats.append("maxDistance", farthestDist);
        stats.done();

        return true;
    }

}  // namespace mongo
