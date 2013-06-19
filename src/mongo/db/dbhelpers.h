/* @file dbhelpers.h

   db helpers are helper functions and classes that let us easily manipulate the local
   database instance in-proc.
*/

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

#include "mongo/pch.h"
#include "mongo/db/client.h"

namespace mongo {

    /**
       all helpers assume locking is handled above them
     */
    namespace Helpers {

        // TODO: Last thing to go for mongod helpers

        void putSingleton(const char *ns, BSONObj obj);

        // TODO: The stuff below is only needed for sharding.

        /* Takes object o, and returns a new object with the
         * same field elements but the names stripped out.  Also,
         * fills in "key" with an ascending keyPattern that matches o
         * Example:
         *    o = {a : 5 , b : 6} -->
         *      sets key= {a : 1, b :1}, returns {"" : 5, "" : 6}
         */
        BSONObj toKeyFormat( const BSONObj& o , BSONObj& key );

        /* Takes a BSONObj indicating the min or max boundary of a range,
         * and a keyPattern corresponding to an index that is useful
         * for locating items in the range, and returns an "extension"
         * of the bound, modified to fit the given pattern.  In other words,
         * it appends MinKey or MaxKey values to the bound, so that the extension
         * has the same number of fields as keyPattern.
         * minOrMax should be -1/+1 to indicate whether the extension
         * corresponds to the min or max bound for the range.
         * Also, strips out the field names to put the bound in key format.
         * Examples:
         *   {a : 55}, {a :1}, -1 --> {"" : 55}
         *   {a : 55}, {a : 1, b : 1}, -1 -> {"" : 55, "" : minKey}
         *   {a : 55}, {a : 1, b : 1}, 1 -> {"" : 55, "" : maxKey}
         *   {a : 55}, {a : 1, b : -1}, -1 -> {"" : 55, "" : maxKey}
         *   {a : 55}, {a : 1, b : -1}, 1 -> {"" : 55, "" : minKey}
         *
         * This function is useful for modifying chunk ranges in sharding,
         * when the shard key is a prefix of the index actually used
         * (also useful when the shard key is equal to the index used,
         * since it strips out the field names).
         */
        BSONObj modifiedRangeBound( const BSONObj& bound ,
                                           const BSONObj& keyPattern ,
                                           int minOrMax );

        /**
         * Takes a range, specified by a min and max, and an index, specified by
         * keyPattern, and removes all the documents in that range found by iterating
         * over the given index. Caller is responsible for insuring that min/max are
         * compatible with the given keyPattern (e.g min={a:100} is compatible with
         * keyPattern={a:1,b:1} since it can be extended to {a:100,b:minKey}, but
         * min={b:100} is not compatible).
         *
         * Caller must hold a write lock on 'ns'
         *
         * Does oplog the individual document deletions.
         */
        long long removeRange( const string& ns , 
                                      const BSONObj& min , 
                                      const BSONObj& max , 
                                      const BSONObj& keyPattern ,
                                      bool maxInclusive = false , 
                                      /* RemoveCallback * callback = 0, */
                                      bool fromMigrate = false );

    };

} // namespace mongo
