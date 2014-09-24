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

#include "mongo/db/geo/s2cursor.h"

#include "mongo/db/index.h"
#include "mongo/db/matcher.h"
#include "mongo/db/geo/s2common.h"

namespace mongo {
    S2Cursor::S2Cursor(const BSONObj &keyPattern, const IndexDetails *details,
                       const BSONObj &query, const vector<GeoQuery> &fields,
                       const S2IndexingParams &params)
        : _details(details), _fields(fields), _params(params), _keyPattern(keyPattern),
          _nscanned(0), _matchTested(0), _geoTested(0) {

        BSONObjBuilder geoFieldsToNuke;
        for (size_t i = 0; i < _fields.size(); ++i) {
            geoFieldsToNuke.append(_fields[i].getField(), "");
        }
        // false means we want to filter OUT geoFieldsToNuke, not filter to include only that.
        _filteredQuery = query.filterFieldsUndotted(geoFieldsToNuke.obj(), false);
        _matcher.reset(new CoveredIndexMatcher(_filteredQuery, keyPattern));
    }

    S2Cursor::~S2Cursor() { }

    CoveredIndexMatcher* S2Cursor::matcher() const { return _matcher.get(); }

    bool S2Cursor::ok() {
        if (NULL == _cursor.get()) {
            // All the magic is in makeUnifiedFRS.  See below.
            // A lot of these arguments are opaque.
            BSONObj frsObj;
            if (!makeFRSObject(&frsObj)) { return false; }
            FieldRangeSet frs(_details->parentNS().c_str(), frsObj, false, false);
            shared_ptr<FieldRangeVector> frv(new FieldRangeVector(frs, _keyPattern, _details->getKeyGenerator(), 1));
            _cursor = Cursor::make(getCollection(_details->parentNS()),
                                                 *_details, frv, 0, 1);
            return advance();
        }
        return _cursor->ok();
    }

    // Make the FieldRangeSet of keys we look for.  Uses coverAsBSON to go from
    // a region to a covering to a set of keys for that covering.
    // Returns false if the FRS object would be empty.
    bool S2Cursor::makeFRSObject(BSONObj *out) {
        BSONObjBuilder frsObjBuilder;
        frsObjBuilder.appendElements(_filteredQuery);

        S2RegionCoverer coverer;

        for (size_t i = 0; i < _fields.size(); ++i) {
            vector<S2CellId> cover;
            double area = _fields[i].getRegion().GetRectBound().Area();
            S2SearchUtil::setCoverLimitsBasedOnArea(area, &coverer, _params.coarsestIndexedLevel);
            coverer.GetCovering(_fields[i].getRegion(), &cover);
            if (0 == cover.size()) { return false; }
            _cellsInCover = cover.size();
            BSONObj fieldRange = S2SearchUtil::coverAsBSON(cover, _fields[i].getField(),
                _params.coarsestIndexedLevel);
            frsObjBuilder.appendElements(fieldRange);
        }

        *out = frsObjBuilder.obj();
        return true;
    }

    BSONObj S2Cursor::current() { return _cursor->current(); }
    BSONObj S2Cursor::currPK() const { return _cursor->currPK(); }
    BSONObj S2Cursor::currKey() const { return _cursor->currKey(); }
    long long S2Cursor::nscanned() const { return _nscanned; }
    bool S2Cursor::getsetdup(const BSONObj &pk) { return _cursor->getsetdup(pk); }

    // This is the actual search.
    bool S2Cursor::advance() {
        for (; _cursor->ok(); _cursor->advance()) {
            ++_nscanned;
            if (_seen.end() != _seen.find(_cursor->currPK())) { continue; }
            _seen.insert(_cursor->currPK().getOwned());

            ++_matchTested;
            MatchDetails details;
            bool matched = _matcher->matchesCurrent(_cursor.get(), &details);
            if (!matched) { continue; }

            const BSONObj &indexedObj = _cursor->current();

            ++_geoTested;
            size_t geoFieldsMatched = 0;
            // OK, cool, non-geo match satisfied.  See if the object actually overlaps w/the geo
            // query fields.
            for (size_t i = 0; i < _fields.size(); ++i) {
                BSONElementSet geoFieldElements;
                indexedObj.getFieldsDotted(_fields[i].getField(), geoFieldElements, false);
                if (geoFieldElements.empty()) { continue; }

                bool match = false;

                for (BSONElementSet::iterator oi = geoFieldElements.begin();
                     !match && (oi != geoFieldElements.end()); ++oi) {
                    if (!oi->isABSONObj()) { continue; }
                    const BSONObj &geoObj = oi->Obj();
                    GeometryContainer geoContainer;
                    uassert(16698, "malformed geometry: " + geoObj.toString(),
                            geoContainer.parseFrom(geoObj));
                    match = _fields[i].satisfiesPredicate(geoContainer);
                }

                if (match) { ++geoFieldsMatched; }
            }

            if (geoFieldsMatched == _fields.size()) {
                // We have a winner!  And we point at it.
                return true;
            }
        }
        return false;
    }

    void S2Cursor::explainDetails(BSONObjBuilder& b) const {
        b << "nscanned" << _nscanned;
        b << "matchTested" << _matchTested;
        b << "geoTested" << _geoTested;
        b << "cellsInCover" << _cellsInCover;
    }
}  // namespace mongo
