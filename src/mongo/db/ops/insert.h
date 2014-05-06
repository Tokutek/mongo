//@file insert.h

/**
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
#include "mongo/bson/bsonobj.h"

namespace mongo {

    class Collection;

    // validate an object before insertion
    void validateInsert(const BSONObj &obj);

    // Insert an object into the given namespace. May modify the object (ie: maybe add _id field). Does not log.
    void insertOneObject(Collection *cl, BSONObj &obj, uint64_t flags = 0);

    // Internal-use only: Does not check magic system collection inserts.
    void _insertObjects(const char *ns, const vector<BSONObj> &objs, bool keepGoing, uint64_t flags, bool logop, bool fromMigrate = false);

    // Insert a vector of objects into the given namespace, logging each operation individually.
    void insertObjects(const char *ns, const vector<BSONObj> &objs, bool keepGoing, uint64_t flags, bool logop, bool fromMigrate = false);

    // Insert an object into the given namespace. Logs the operation.
    void insertObject(const char *ns, const BSONObj &obj, uint64_t flags = 0, bool logop = true, bool fromMigrate = false);
    
}  // namespace mongo
