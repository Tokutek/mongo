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
#include "mongo/db/index/hashed_key_generator.h"
#include "mongo/db/storage/assert_ids.h"

namespace mongo {

    /* Takes a BSONElement, seed and hashVersion, and outputs the
     * 64-bit hash used for this index
     * E.g. if the element is {a : 3} this outputs v1-hash(3)
     * */
    long long int HashedKeyGenerator::makeSingleKey(const BSONElement &e,
                                                    const HashSeed &seed,
                                                    const HashVersion &v) {
        massert( 16245, "Only HashVersion 0 has been defined", v == 0 );
        return BSONElementHasher::hash64( e , seed );
    }

    void HashedKeyGenerator::getKeys(const BSONObj &obj, BSONObjSet &keys) const {
        const char *hashedFieldPtr = _fieldNames[0];
        const BSONElement &fieldVal = obj.getFieldDottedOrArray( hashedFieldPtr );
        uassert( storage::ASSERT_IDS::CannotHashArrays,
                 "Error: hashed indexes do not currently support array values",
                 fieldVal.type() != Array );

        if (!fieldVal.eoo()) {
            BSONObj key = BSON("" << makeSingleKey(fieldVal, _seed, _hashVersion));
            keys.insert(key);
        } else if (!_sparse) {
            BSONObj key = BSON("" << makeSingleKey(nullElt, _seed, _hashVersion));
            keys.insert(key);
        }
    }

} // namespace mongo
