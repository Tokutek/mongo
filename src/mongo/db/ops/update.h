//@file update.h

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
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/pch.h"

#include "mongo/db/jsobj.h"

namespace mongo {

    class Collection;

    struct UpdateResult {
        const bool existing; // if existing objects were modified
        const bool mod;      // was this a $ mod
        const long long num; // how many objects touched
        OID upserted;        // if something was upserted, the new _id of the object

        UpdateResult(const bool e, const bool m,
                     const unsigned long long n, const BSONObj &upsertedObj) :
            existing(e), mod(m), num(n) {
            upserted.clear();
            const BSONElement id = upsertedObj["_id"];
            if (!e && n == 1 && id.type() == jstOID) {
                upserted = id.OID();
            }
        }
    };

    BSONObj invertUpdateMods(const BSONObj &updateobj);

    void updateOneObject(Collection *cl, const BSONObj &pk, 
                         const BSONObj &oldObj, BSONObj &newObj, 
                         const BSONObj &updateobj,
                         const bool fromMigrate,
                         uint64_t flags);

    UpdateResult updateObjects(const char *ns,
                               const BSONObj &updateobj, const BSONObj &pattern,
                               const bool upsert, const bool multi,
                               const bool fromMigrate = false);

}  // namespace mongo
