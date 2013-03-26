//@file insert.cpp

/**
 *    Copyright (C) 2012 Tokutek Inc.
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

#include "pch.h"

#include "mongo/db/client.h"
#include "mongo/db/database.h"
#include "mongo/db/oplog.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/jsobjmanipulator.h"
#include "mongo/db/ops/insert.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    static int handle_system_collection_insert(const char *ns, const BSONObj &obj) {
        // Trying to insert into a system collection.  Fancy side-effects go here:
        // TODO: see insert_checkSys
        if (mongoutils::str::endsWith(ns, ".system.indexes")) {
            // obj is something like this:
            // { _id: ObjectId('511d34f6d3080c48017a14d0'), ns: "test.leif", key: { a: -1.0 }, name: "a_-1", unique: true }
            const string &coll = obj["ns"].String();
            const bool collIsNew = nsdetails(coll.c_str()) == NULL;
            NamespaceDetails *details = nsdetails_maybe_create(coll.c_str());
            BSONObj key = obj["key"].Obj();
            int i = details->findIndexByKeyPattern(key);
            if (i >= 0) {
                return ASSERT_ID_DUPKEY;
            } else {
                try {
                    details->createIndex(obj);
                } catch (DBException &e) {
                    if (collIsNew) {
                        // We created the collection above just to create this index, but creating the index failed, so we should also roll back the collection creation.
                        // This has some transaction-ignorant pieces in the NamespaceIndex so we have to manually undo them here.
                        nsindex(coll.c_str())->kill_ns(coll.c_str());
                    }
                    throw;
                }
            }
        } else if (legalClientSystemNS(ns, true)) {
            if (mongoutils::str::endsWith(ns, ".system.users")) {
                uassert( 14051 , "system.users entry needs 'user' field to be a string", obj["user"].type() == String );
                uassert( 14052 , "system.users entry needs 'pwd' field to be a string", obj["pwd"].type() == String );
                uassert( 14053 , "system.users entry needs 'user' field to be non-empty", obj["user"].String().size() );
                uassert( 14054 , "system.users entry needs 'pwd' field to be non-empty", obj["pwd"].String().size() );
            }
        } else {
            uasserted(16459, str::stream() << "attempt to insert in system namespace '" << ns << "'");
        }
        return 0;
    }

    void insertOneObject(NamespaceDetails *details, NamespaceDetailsTransient *nsdt, BSONObj &obj, bool overwrite) {
        details->insertObject(obj, overwrite);
        if (nsdt != NULL) {
            nsdt->notifyOfWriteOp();
        }
    }

    void insertObjects(const char *ns, const vector<BSONObj> &objs, bool overwrite, bool keepGoing) {
        if (mongoutils::str::contains(ns, "system.")) {
            uassert(10095, "attempt to insert in reserved database name 'system'", !mongoutils::str::startsWith(ns, "system."));
            massert(16462, "attempted to insert multiple objects into a system namspace at once", objs.size() == 1);
            if (handle_system_collection_insert(ns, objs[0]) != 0) {
                return;
            }
        }

        NamespaceDetails *details = nsdetails_maybe_create(ns);
        NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get(ns);
        for (size_t i = 0; i < objs.size(); i++) {
            const BSONObj &obj = objs[i];
            try {
                uassert( 10059 , "object to insert too large", obj.objsize() <= BSONObjMaxUserSize);
                BSONObjIterator i( obj );
                while ( i.more() ) {
                    BSONElement e = i.next();
                    uassert( 13511 , "document to insert can't have $ fields" , e.fieldName()[0] != '$' );
                }
                uassert( 16440 ,  "_id cannot be an array", obj["_id"].type() != Array );

                BSONObj objModified = obj;
                BSONElementManipulator::lookForTimestamps(objModified);
                insertOneObject(details, nsdt, objModified, overwrite); // may add _id field
                logOp("i", ns, objModified);
            } catch (const UserException &) {
                if (!keepGoing || i == objs.size() - 1) {
                    throw;
                }
            }
        }
    }

    void insertObject(const char *ns, const BSONObj &obj, bool overwrite) {
        vector<BSONObj> objs(1);
        objs[0] = obj;
        insertObjects(ns, objs, overwrite);
    }
    
} // namespace mongo
