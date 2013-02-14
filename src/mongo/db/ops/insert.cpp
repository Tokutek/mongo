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
#include "mongo/db/ops/insert.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    static void handle_system_collection_insert(const char *ns, const BSONObj &obj) {
        // Trying to insert into a system collection.  Fancy side-effects go here:
        // TODO: see insert_checkSys
        if (mongoutils::str::endsWith(ns, ".system.indexes")) {
            Client::RootTransaction txn;
            // obj is something like this:
            // { _id: ObjectId('511d34f6d3080c48017a14d0'), ns: "test.leif", key: { a: -1.0 }, name: "a_-1", unique: true }
            const string &coll = obj["ns"].String();
            NamespaceDetails *details = nsdetails_maybe_create(coll.c_str());
            wunimplemented("make sure we don't try to create the same index twice");
            details->createIndex(obj);
            txn.commit();
        }
    }

    void insertObject(const char *ns, const BSONObj &obj) {
        Client::WriteContext ctx(ns);
        if (mongoutils::str::contains(ns, "system.")) {
            uassert(10095, "attempt to insert in reserved database name 'system'", !mongoutils::str::startsWith(ns, "system."));
            handle_system_collection_insert(ns, obj);
        }
        NamespaceDetails *details = nsdetails_maybe_create(ns);
        // TODO: need some transactional atomicity around these two?
        details->insert(ns, obj, true);
        logOp("i", ns, obj);
    }
    
} // namespace mongo
