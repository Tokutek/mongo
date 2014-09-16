// index.h

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

#pragma once

#include <vector>

#include "mongo/pch.h"
#include "mongo/db/hasher.h"
#include "mongo/db/index.h"
#include "mongo/db/key_generator.h"
#include "mongo/util/percentage_progress_meter.h"

namespace mongo {

    /* This is an index where the keys are hashes of a given field.
     *
     * Optional arguments:
     *  "seed" : int (default = 0, a seed for the hash function)
     *  "hashVersion : int (default = 0, determines which hash function to use)
     *
     * Example use in the mongo shell:
     * > db.foo.ensureIndex({a : "hashed"}, {seed : 3, hashVersion : 0})
     *
     * LIMITATION: Only works with a single field. The HashedIndex
     * constructor uses uassert to ensure that the spec has the form
     * {<fieldname> : "hashed"}, and not, for example,
     * { a : "hashed" , b : 1}
     *
     * LIMITATION: Cannot be used as a unique index.
     * The HashedIndex constructor uses uassert to ensure that
     * the spec does not contain {"unique" : true}
     *
     * LIMITATION: Cannot be used to index arrays.
     * The getKeys function uasserts that value being inserted
     * is not an array.  This index will not be built if any
     * array values of the hashed field exist.
     */
    class HashedIndex : public IndexInterface {
    public:
        HashedIndex(const BSONObj &info);

        ~HashedIndex();

        // @return the "special" name for this index.
        const string &getSpecialIndexName() const {
            static string name = "hashed";
            return name;
        }

        Suitability suitability(const FieldRangeSet &queryConstraints,
                                const BSONObj &order) const;

        /* The newCursor method works for suitable queries by generating a IndexCursor
         * using the hash of point-intervals parsed by FieldRangeSet.
         * For unsuitable queries it just instantiates a cursor over the whole index.
         */
        shared_ptr<mongo::Cursor> newCursor(const BSONObj &query,
                                            const BSONObj &order,
                                            const int numWanted = 0) const;

        // A missing field is represented by a hashed null element.
        BSONElement missingField() const {
            return _hashedNullObj.firstElement();
        }

    private:
        const string _hashedField;
        const HashSeed _seed;
        // In case we have hashed indexes based on other hash functions in
        // the future, we store a hashVersion number.
        const HashVersion _hashVersion;
        const BSONObj _hashedNullObj;
    };

} // namespace mongo

