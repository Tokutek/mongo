// db/geo/haystack.cpp

/**
 *    Copyright (C) 2008-2012 10gen Inc.
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

#include "mongo/db/collection.h"
#include "mongo/db/cursor.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/index.h"
#include "mongo/db/index/haystack.h"
#include "mongo/db/index/haystack_descriptor.h"
#include "mongo/db/index/haystack_key_generator.h"
#include "mongo/db/commands.h"
#include "mongo/db/geo/core.h"
#include "mongo/db/geo/hash.h"
#include "mongo/db/geo/shapes.h"
#include "mongo/util/timer.h"

/**
 * Provides the geoHaystack index type and the command "geoSearch."
 * Examines all documents in a given radius of a given point.
 * Returns all documents that match a given search restriction.
 * See http://dochub.mongodb.org/core/haystackindexes
 *
 * Use when you want to look for restaurants within 25 miles with a certain name.
 * Don't use when you want to find the closest open restaurants; see 2d.cpp for that.
 */
namespace mongo {

    static const string GEOSEARCHNAME = "geoHaystack";

    HaystackIndex::HaystackIndex(const BSONObj &info) :
        IndexInterface(info) {

        BSONElement e = _info["bucketSize"];
        uassert(13321, "need bucketSize", e.isNumber());
        _bucketSize = e.numberDouble();
        uassert(17371, "bucketSize cannot be zero", _bucketSize != 0.0);

        // Example:
        // db.foo.ensureIndex({ pos : "geoHaystack", type : 1 }, { bucketSize : 1 })
        BSONObjIterator i(_keyPattern);
        while (i.more()) {
            BSONElement e = i.next();
            if (e.type() == String && GEOSEARCHNAME == e.valuestr()) {
                uassert(13314, "can't have more than one geo field", _geoField.size() == 0);
                uassert(13315, "the geo field has to be first in index",
                        _otherFields.size() == 0);
                _geoField = e.fieldName();
            } else {
                // TODO(hk): Do we want to do any checking on e.type and e.valuestr?
                uassert(13326, "geoSearch can only have 1 non-geo field for now",
                        _otherFields.size() == 0);
                _otherFields.push_back(e.fieldName());
            }
        }

        uassert(13316, "no geo field specified", _geoField.size());
        // XXX: Fix documentation that says the other field is optional; code says it's mandatory.
        uassert(13317, "no non-geo fields specified", _otherFields.size());

        // Create a haystack descriptor that will store _geoField and _otherFields
        _descriptor.reset(new HaystackDescriptor(_keyPattern, _geoField, _otherFields, _bucketSize, _sparse, _clustering));

        // Reset the key generator to be a haystack key generator
        _keyGenerator.reset(new HaystackKeyGenerator(_geoField, _otherFields, _bucketSize, _sparse));
    }

    class GeoHaystackSearchHopper {
    public:
        /**
         * Constructed with a point, a max distance from that point, and a max number of
         * matched points to store.
         * @param n  The centroid that we're searching
         * @param maxDistance  The maximum distance to consider from that point
         * @param limit  The maximum number of results to return
         * @param geoField  Which field in the provided pk has the point to test.
         */
        GeoHaystackSearchHopper(const BSONObj& nearObj, double maxDistance, unsigned limit,
                                const string& geoField)
            : _near(nearObj), _maxDistance(maxDistance), _limit(limit), _geoField(geoField) { }

        // Consider the point given by obj and keep it if it's within _maxDistance (and we have space for
        // it)
        void consider(const BSONObj &obj) {
            if (limitReached()) return;
            Point p(obj.getFieldDotted(_geoField));
            if (distance(_near, p) > _maxDistance)
                return;
            _resultObjs.push_back(obj.getOwned());
        }

        int appendResultsTo(BSONArrayBuilder* b) {
            for (unsigned i = 0; i <_resultObjs.size(); i++)
                b->append(_resultObjs[i]);
            return _resultObjs.size();
        }

        // Have we stored as many points as we can?
        const bool limitReached() const {
            return _resultObjs.size() >= _limit;
        }
    private:
        Point _near;
        double _maxDistance;
        unsigned _limit;
        const string _geoField;
        vector<BSONObj> _resultObjs;
    };

    void HaystackIndex::searchCommand(Collection* cl,
                                      const BSONObj& n /*near*/, double maxDistance, const BSONObj& search,
                                      BSONObjBuilder& result, unsigned limit) {
        Timer t;

        LOG(1) << "SEARCH near:" << n << " maxDistance:" << maxDistance
            << " search: " << search << endl;
        int x, y;
        {
            BSONObjIterator i(n);
            x = hash(_bucketSize, i.next());
            y = hash(_bucketSize, i.next());
        }
        int scale = static_cast<int>(ceil(maxDistance / _bucketSize));

        GeoHaystackSearchHopper hopper(n, maxDistance, limit, _geoField);

        long long indexMatches = 0;

        // TODO(hk): Consider starting with a (or b)=0, then going to a=+-1, then a=+-2, etc.
        // Would want a HaystackKeyIterator or similar for this, but it'd be a nice
        // encapsulation allowing us to S2-ify this trivially/abstract the key details.
        for (int a = -scale; a <= scale && !hopper.limitReached(); ++a) {
            for (int b = -scale; b <= scale && !hopper.limitReached(); ++b) {
                BSONObjBuilder bb;
                bb.append("", makeString(x + a, y + b));

                for (unsigned i = 0; i < _otherFields.size(); i++) {
                    // See if the non-geo field we're indexing on is in the provided search term.
                    BSONElement e = search.getFieldDotted(_otherFields[i]);
                    if (e.eoo())
                        bb.appendNull("");
                    else
                        bb.appendAs(e, "");
                }

                BSONObj key = bb.obj();

                GEOQUADDEBUG("KEY: " << key);

                // TODO(hk): this keeps a set of all PKs seen in this pass so that we don't
                // consider the element twice.  Do we want to instead store a hash of the set?
                // Is this often big?
                set<BSONObj> thisPass;

                // Lookup from key to key, inclusive.
                shared_ptr<Cursor> cursor = Cursor::make(cl,
                                                         *this,
                                                         key,
                                                         key,
                                                         true,
                                                         1);
                while (cursor->ok() && !hopper.limitReached()) {
                    pair<set<BSONObj>::iterator, bool> p = thisPass.insert(cursor->currPK().getOwned());
                    // If a new element was inserted (haven't seen the PK before), p.second
                    // is true.
                    if (p.second) {
                        hopper.consider(cursor->current());
                        GEOQUADDEBUG("\t" << cursor->current());
                        indexMatches++;
                    }
                    cursor->advance();
                }
            }
        }

        BSONArrayBuilder arr(result.subarrayStart("results"));
        int num = hopper.appendResultsTo(&arr);
        arr.done();

        {
            BSONObjBuilder b(result.subobjStart("stats"));
            b.append("time", t.millis());
            b.appendNumber("indexMatches", indexMatches);
            b.append("n", num);
            b.done();
        }
    }

    int HaystackIndex::hash(double bucketSize, const BSONElement& e) {
        uassert(13322, "geo field is not a number", e.isNumber());
        return hash(bucketSize, e.numberDouble());
    }

    int HaystackIndex::hash(double bucketSize, double d) {
        d += 180;
        d /= bucketSize;
        return static_cast<int>(d);
    }

    string HaystackIndex::makeString(int hashedX, int hashedY) {
        stringstream ss;
        ss << hashedX << "_" << hashedY;
        return ss.str();
    }
}
