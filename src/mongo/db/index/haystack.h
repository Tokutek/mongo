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

#pragma once

#include "pch.h"

#include <vector>

#include "mongo/db/jsobj.h"
#include "mongo/db/index.h"

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

    /**
     * Provides the IndexType for geoSearch.
     * Maps (lat, lng) to the bucketSize-sided square bucket that contains it.
     * Usage:
     * db.foo.ensureIndex({ pos : "geoHaystack", type : 1 }, { bucketSize : 1 })
     *   pos is the name of the field to be indexed that has lat/lng data in an array.
     *   type is the name of the secondary field to be indexed. 
     *   bucketSize specifies the dimension of the square bucket for the data in pos.
     * ALL fields are mandatory.
     */
    class HaystackIndex : public IndexInterface {
    public:
        HaystackIndex(const BSONObj &info);
        virtual ~HaystackIndex() { }

        // @return the "special" name for this index.
        const string &getSpecialIndexName() const {
            static const string name = "geoHaystack";
            return name;
        }

        shared_ptr<Cursor> newCursor(const BSONObj& query, const BSONObj& order,
                                     int numWanted) const {
            msgasserted(17373, "bug: newCursor called on a haystack index");
        }

        void searchCommand(Collection* cl,
                           const BSONObj& n /*near*/, double maxDistance, const BSONObj& search,
                           BSONObjBuilder& result, unsigned limit);

    private:
        // static, since these are all called by HaystackKeyGenerator
        static int hash(double bucketSize, const BSONElement& e);

        static int hash(double bucketSize, double d);

        static string makeString(int hashedX, int hashedY);

        // Build a new BSONObj with root in it.  If e is non-empty, append that to the key.  Insert
        // the BSONObj into keys.
        void addKey(const string& root, const BSONElement& e, BSONObjSet& keys) const;

        string _geoField;
        vector<string> _otherFields;
        double _bucketSize;

        friend class HaystackKeyGenerator;
    };
}
