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
 */

#include "mongo/pch.h"

#include "mongo/db/jsobj.h"
#include "mongo/db/ops/update_internal.h"

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

    bool updateOneObjectWithMods(Collection *cl, const BSONObj &pk, 
                         const BSONObj &updateobj,
                         const bool fromMigrate,
                         uint64_t flags,
                         ModSet* useMods);
    
    void updateOneObject(Collection *cl, const BSONObj &pk, 
                         const BSONObj &oldObj, BSONObj &newObj, 
                         const bool fromMigrate,
                         uint64_t flags);

    UpdateResult updateObjects(const char *ns,
                               const BSONObj &updateobj, const BSONObj &pattern,
                               const bool upsert, const bool multi,
                               const bool fromMigrate = false);

}  // namespace mongo
