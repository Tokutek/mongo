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
#include "mongo/db/jsobj.h"
#include "mongo/db/index/2d_key_generator.h"

namespace mongo {

    TwoDKeyGenerator::TwoDKeyGenerator(const string &geo,
                                       const vector<pair<string, int> > &other,
                                       const GeoHashConverter::Parameters &params,
                                       const bool sparse) :
        KeyGenerator(vector<const char *>(), sparse),
        _geo(geo),
        _other(other),
        _geoHashConverter(new GeoHashConverter(params)) {
    }

    /** Finds the key objects to put in an index */
    void TwoDKeyGenerator::getKeys(const BSONObj &obj, BSONObjSet &keys) const {
        _getKeys(obj, &keys, NULL);
    }

    /** Finds all locations in a geo-indexed object */
    void TwoDKeyGenerator::getGeoLocs(const BSONObj &obj, vector<BSONObj> &locs) const {
        _getKeys(obj, NULL, &locs);
    }

    /** Finds the key objects and/or locations for a geo-indexed object */
    void TwoDKeyGenerator::_getKeys(const BSONObj &obj, BSONObjSet *keys, vector<BSONObj> *locs) const {
        BSONElementMSet bSet;

        // Get all the nested location fields, but don't return individual elements from
        // the last array, if it exists.
        obj.getFieldsDotted(_geo.c_str(), bSet, false);

        if (bSet.empty())
            return;

        for (BSONElementMSet::iterator setI = bSet.begin(); setI != bSet.end(); ++setI) {
            BSONElement geo = *setI;

            if (geo.eoo() || !geo.isABSONObj())
                continue;

            //
            // Grammar for location lookup:
            // locs ::= [loc,loc,...,loc]|{<k>:loc,<k>:loc,...,<k>:loc}|loc
            // loc  ::= { <k1> : #, <k2> : # }|[#, #]|{}
            //
            // Empty locations are ignored, preserving single-location semantics
            //

            BSONObj embed = geo.embeddedObject();
            if (embed.isEmpty())
                continue;

            // Differentiate between location arrays and locations
            // by seeing if the first element value is a number
            bool singleElement = embed.firstElement().isNumber();

            BSONObjIterator oi(embed);

            while (oi.more()) {
                BSONObj locObj;

                if (singleElement) {
                    locObj = embed;
                } else {
                    BSONElement locElement = oi.next();

                    uassert(13654, str::stream() << "location object expected, location "
                                                   "array not in correct format",
                            locElement.isABSONObj());

                    locObj = locElement.embeddedObject();
                    if(locObj.isEmpty())
                        continue;
                }

                BSONObjBuilder b(64);

                // Remember the actual location object if needed
                if (locs)
                    locs->push_back(locObj);

                // Stop if we don't need to get anything but location objects
                if (!keys) {
                    if (singleElement) break;
                    else continue;
                }

                _geoHashConverter->hash(locObj, &obj).appendToBuilder(&b, "");

                // Go through all the other index keys
                for (vector<pair<string, int> >::const_iterator i = _other.begin();
                     i != _other.end(); ++i) {
                    // Get *all* fields for the index key
                    BSONElementSet eSet;
                    obj.getFieldsDotted(i->first, eSet);

                    if (eSet.size() == 0)
                        b.appendAs(nullElt, "");
                    else if (eSet.size() == 1)
                        b.appendAs(*(eSet.begin()), "");
                    else {
                        // If we have more than one key, store as an array of the objects
                        BSONArrayBuilder aBuilder;

                        for (BSONElementSet::iterator ei = eSet.begin(); ei != eSet.end();
                             ++ei) {
                            aBuilder.append(*ei);
                        }

                        b.append("", aBuilder.arr());
                    }
                }
                keys->insert(b.obj());
                if(singleElement) break;
            }
        }
    }

} // namespace mongo
