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

#pragma once

#include <vector>

#include "mongo/pch.h"
#include "mongo/db/jsobj.h"

namespace mongo {

    // Generates keys for a standard index.
    class KeyGenerator : boost::noncopyable {
    public:
        KeyGenerator(const vector<const char *> &fieldNames,
                     const bool sparse) :
            _fieldNames(fieldNames),
            _sparse(sparse) {
        }
        virtual ~KeyGenerator() { }

        virtual void getKeys(const BSONObj &obj, BSONObjSet &keys) const;

        // Needed for some strange code in geo.
        virtual void getGeoLocs(const BSONObj &obj, vector<BSONObj> &locs) const {
            msgasserted(0, "bug: called getKeys with vector arg on non-2d index");
        }

    private:
        /**
         * @param arrayNestedArray - set if the returned element is an array nested directly within arr.
         */
        static BSONElement extractNextElement( const BSONObj &obj, const BSONObj &arr,
                                               const char *&field, bool &arrayNestedArray );
        
        static void _getKeysArrEltFixed( vector<const char*> &fieldNames , vector<BSONElement> &fixed ,
                                         const BSONElement &arrEntry, const bool sparse, BSONObjSet &keys, int numNotFound,
                                         const BSONElement &arrObjElt, const set< unsigned > &arrIdxs,
                                         bool mayExpandArrayUnembedded );
        
        /**
         * @param fieldNames - fields to index, may be postfixes in recursive calls
         * @param fixed - values that have already been identified for their index fields
         * @param obj - object from which keys should be extracted, based on names in fieldNames
         * @param keys - set where index keys are written
         * @param numNotFound - number of index fields that have already been identified as missing
         * @param array - array from which keys should be extracted, based on names in fieldNames
         *        If obj and array are both nonempty, obj will be one of the elements of array.
         */        
        static void _getKeys( vector<const char*> fieldNames , vector<BSONElement> fixed ,
                              const BSONObj &obj, const bool sparse, BSONObjSet &keys, int numNotFound = 0,
                              const BSONObj &array = BSONObj() );

    protected:
        vector<const char *> _fieldNames;
        const bool _sparse;
    };

} // namespace mongo
