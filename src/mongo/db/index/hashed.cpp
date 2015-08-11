/** @file index.cpp */

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

#include "mongo/pch.h"

#include "mongo/db/collection.h"
#include "mongo/db/cursor.h"
#include "mongo/db/index.h"
#include "mongo/db/index/hashed.h"
#include "mongo/db/index/hashed_descriptor.h"
#include "mongo/db/index/hashed_key_generator.h"
#include "mongo/db/storage/assert_ids.h"

namespace mongo {

    HashedIndex::HashedIndex(const BSONObj &info) :
        IndexInterface(info),
        _hashedField(_keyPattern.firstElement().fieldName()),
        // Default seed/version to 0 if not specified or not an integer.
        _seed(_info["seed"].numberInt()),
        _hashVersion(_info["hashVersion"].numberInt()),
        _hashedNullObj(BSON("" << HashedKeyGenerator::makeSingleKey(nullElt, _seed, _hashVersion)))
    {
        // change these if single-field limitation lifted later
        uassert( 16241, "Currently only single field hashed index supported.",
                 _keyPattern.nFields() == 1 );
        uassert( 16242, "Currently hashed indexes cannot guarantee uniqueness. Use a regular index.",
                 !unique() );

        // Create a hashed descriptor so that the dictionary's descriptor gets _seed
        _descriptor.reset(new HashedDescriptor(_keyPattern, _seed, _hashVersion, _sparse, _clustering));

        // Reset the key generator to be an s2 key generator
        vector<const char *> fieldNames;
        _descriptor->fieldNames(fieldNames);
        _keyGenerator.reset(new HashedKeyGenerator(fieldNames, _seed, _hashVersion, _sparse));
    }

    HashedIndex::~HashedIndex() { }

    IndexDetails::Suitability HashedIndex::suitability(const FieldRangeSet &queryConstraints,
                            const BSONObj &order) const {
        if (queryConstraints.isPointIntervalSet(_hashedField)) {
            return HELPFUL;
        }
        return USELESS;
    }

    /* The newCursor method works for suitable queries by generating a IndexCursor
     * using the hash of point-intervals parsed by FieldRangeSet.
     * For unsuitable queries it just instantiates a cursor over the whole index.
     */
    shared_ptr<mongo::Cursor> HashedIndex::newCursor(const BSONObj &query,
                                                     const BSONObj &order,
                                                     const int numWanted) const {

        // Use FieldRangeSet to parse the query into a vector of intervals
        // These should be point-intervals if this cursor is ever used
        // So the FieldInterval vector will be, e.g. <[1,1], [3,3], [6,6]>
        FieldRangeSet frs("" , query , true, true);
        const vector<FieldInterval> &intervals = frs.range(_hashedField.c_str()).intervals();

        // Force a match of the query against the actual document by giving
        // the cursor a matcher with an empty indexKeyPattern.  This insures the
        // index is not used as a covered index.
        // NOTE: this forcing is necessary due to potential hash collisions
        const shared_ptr<CoveredIndexMatcher> forceDocMatcher(
                                                              new CoveredIndexMatcher(query, BSONObj()));

        Collection *cl = getCollection(parentNS());

        // Construct a new query based on the hashes of the previous point-intervals
        // e.g. {a : {$in : [ hash(1) , hash(3) , hash(6) ]}}
        BSONObjBuilder newQueryBuilder;
        BSONObjBuilder inObj(newQueryBuilder.subobjStart(_hashedField));
        BSONArrayBuilder inArray(inObj.subarrayStart("$in"));
        for (vector<FieldInterval>::const_iterator i = intervals.begin();
             i != intervals.end(); ++i ){
            if (!i->equality()){
                const shared_ptr<mongo::Cursor> cursor =
                    mongo::Cursor::make(cl, *this, 1);
                cursor->setMatcher(forceDocMatcher);
                return cursor;
            }
            inArray.append(HashedKeyGenerator::makeSingleKey(i->_lower._bound, _seed, _hashVersion));
        }
        inArray.done();
        inObj.done();

        // Use the point-intervals of the new query to create an index cursor
        const BSONObj newQuery = newQueryBuilder.obj();
        FieldRangeSet newfrs("" , newQuery, true, true);
        shared_ptr<FieldRangeVector> newVector(new FieldRangeVector(newfrs, _keyPattern, getKeyGenerator(), 1));

        const shared_ptr<mongo::Cursor> cursor =
            mongo::Cursor::make(cl, *this, newVector, false, 1, numWanted);
        cursor->setMatcher(forceDocMatcher);
        return cursor;
    }

} // namespace mongo
