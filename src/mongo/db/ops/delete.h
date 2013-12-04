// delete.h

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
#include "mongo/db/jsobj.h"

namespace mongo {

    class NamespaceDetails;

    void deleteOneObject(NamespaceDetails *details, const BSONObj &pk, const BSONObj &obj, uint64_t flags = 0);

    /**
     * Takes a range, specified by a min and max, and an index, specified by
     * keyPattern, and removes all the documents in that range found by iterating
     * over the given index. Caller is responsible for insuring that min/max are
     * compatible with the given keyPattern (e.g min={a:100} is compatible with
     * keyPattern={a:1,b:1} since it can be extended to {a:100,b:minKey}, but
     * min={b:100} is not compatible).
     *
     * Does oplog the individual document deletions.
     */
    long long deleteIndexRange(const string &ns,
                               const BSONObj &min, 
                               const BSONObj &max, 
                               const BSONObj &keyPattern,
                               const bool maxInclusive = false, 
                               const bool fromMigrate = false);

    // System-y version of deleteObjects that allows you to delete from the system collections, used to be god = true.
    long long _deleteObjects(const char *ns, BSONObj pattern, bool justOne, bool logop);

    // If justOne is true, deletedId is set to the id of the deleted object.
    long long deleteObjects(const char *ns, BSONObj pattern, bool justOne, bool logop = false);

}
