//@file insert.cpp

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

#include "pch.h"

#include "mongo/db/client.h"
#include "mongo/db/database.h"
#include "mongo/db/oplog.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/jsobjmanipulator.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    void insertOneObject(NamespaceDetails *details, BSONObj &obj, uint64_t flags) {
        details->insertObject(obj, flags);
        details->notifyOfWriteOp();
    }

    // Does not check magic system collection inserts.
    void _insertObjects(const char *ns, const vector<BSONObj> &objs, bool keepGoing, uint64_t flags, bool logop ) {
        NamespaceDetails *details = getAndMaybeCreateNS(ns, logop);
        for (size_t i = 0; i < objs.size(); i++) {
            const BSONObj &obj = objs[i];
            try {
                uassert( 10059 , "object to insert too large", obj.objsize() <= BSONObjMaxUserSize);
                BSONObjIterator i( obj );
                while ( i.more() ) {
                    BSONElement e = i.next();
                    // check no $ modifiers.  note we only check top level.
                    // (scanning deep would be quite expensive)
                    uassert( 13511 , "document to insert can't have $ fields" , e.fieldName()[0] != '$' );

                    // check no regexp for _id (SERVER-9502)
                    if (str::equals(e.fieldName(), "_id")) {
                        uassert(17033, "can't use a regex for _id", e.type() != RegEx);
                    }
                }
                uassert( 16440 ,  "_id cannot be an array", obj["_id"].type() != Array );

                BSONObj objModified = obj;
                BSONElementManipulator::lookForTimestamps(objModified);
                if (details->isCapped() && logop) {
                    // unfortunate hack we need for capped collections
                    // we do this because the logic for generating the pk
                    // and what subsequent rows to delete are buried in the
                    // namespace details object. There is probably a nicer way
                    // to do this, but this works.
                    details->insertObjectIntoCappedAndLogOps(objModified, flags);
                    details->notifyOfWriteOp();
                }
                else {
                    insertOneObject(details, objModified, flags); // may add _id field
                    if (logop) {
                        OpLogHelpers::logInsert(ns, objModified);
                    }
                }
            } catch (const UserException &) {
                if (!keepGoing || i == objs.size() - 1) {
                    throw;
                }
            }
        }
    }

    static BSONObj stripDropDups(const BSONObj &obj) {
        BSONObjBuilder b;
        for (BSONObjIterator it(obj); it.more(); ) {
            BSONElement e = it.next();
            if (StringData(e.fieldName()) == "dropDups") {
                warning() << "dropDups is not supported because it deletes arbitrary data." << endl;
                warning() << "We'll proceed without it but if there are duplicates, the index build will fail." << endl;
            } else {
                b.append(e);
            }
        }
        return b.obj();
    }

    void insertObjects(const char *ns, const vector<BSONObj> &objs, bool keepGoing, uint64_t flags, bool logop ) {
        StringData _ns(ns);
        if (NamespaceString::isSystem(_ns)) {
            massert(16748, "need transaction to run insertObjects", cc().txnStackSize() > 0);
            uassert(10095, "attempt to insert in reserved database name 'system'", nsToDatabaseSubstring(_ns) != "system");
            massert(16750, "attempted to insert multiple objects into a system namspace at once", objs.size() == 1);

            // Trying to insert into a system collection.  Fancy side-effects go here:
            if (nsToCollectionSubstring(ns) == "system.indexes") {
                BSONObj obj = stripDropDups(objs[0]);
                NamespaceDetails *d = getAndMaybeCreateNS(obj["ns"].Stringdata(), logop);
                bool ok = d->ensureIndex(obj);
                if (!ok) {
                    // Already had that index
                    return;
                }

                // Now we have to actually insert that document into system.indexes, we may have
                // modified it with stripDropDups.
                vector<BSONObj> newObjs;
                newObjs.push_back(obj);
                _insertObjects(ns, newObjs, keepGoing, flags, logop);
                return;
            } else if (!legalClientSystemNS(ns, true)) {
                uasserted(16459, str::stream() << "attempt to insert in system namespace '" << ns << "'");
            }
        }
        _insertObjects(ns, objs, keepGoing, flags, logop);
    }

    void insertObject(const char *ns, const BSONObj &obj, uint64_t flags, bool logop) {
        vector<BSONObj> objs(1);
        objs[0] = obj;
        insertObjects(ns, objs, false, flags, logop);
    }

} // namespace mongo
