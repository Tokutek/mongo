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
         * same field elements but the names stripped out.
         * Example:
         *    o = {a : 5 , b : 6} --> {"" : 5, "" : 6}
         */
        BSONObj toKeyFormat( const BSONObj& o );

        /* Takes object o, and infers an ascending keyPattern with the same fields as o
         * Example:
         *    o = {a : 5 , b : 6} --> {a : 1 , b : 1 }
         */
        BSONObj inferKeyPattern( const BSONObj& o );

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
