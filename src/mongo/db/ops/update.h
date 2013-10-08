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
#include "mongo/db/curop.h"
#include "mongo/db/query_plan_selection_policy.h"

namespace mongo {

    class NamespaceDetails;

    // ---------- public -------------

    struct UpdateResult {
        const bool existing; // if existing objects were modified
        const bool mod;      // was this a $ mod
        const long long num; // how many objects touched
        OID upserted;  // if something was upserted, the new _id of the object

        UpdateResult( bool e, bool m, unsigned long long n , const BSONObj& upsertedObject )
            : existing(e) , mod(m), num(n) {
            upserted.clear();
            BSONElement id = upsertedObject["_id"];
            if ( ! e && n == 1 && id.type() == jstOID ) {
                upserted = id.OID();
            }
        }
    };
    
    struct LogOpUpdateDetails {
        LogOpUpdateDetails(bool log = false, bool m = false) :
            logop(log), fromMigrate(m) {
        }
        const bool logop;
        const bool fromMigrate;
    };

    void updateOneObject(
        NamespaceDetails *d, 
        const BSONObj &pk, 
        const BSONObj &oldObj, 
        const BSONObj &newObj, 
        const LogOpUpdateDetails &logDetails,
        uint64_t flags = 0
        );

    /* returns true if an existing object was updated, false if no existing object was found.
       multi - update multiple objects - mostly useful with things like $set
       su - allow access to system namespaces (super user)
    */
    UpdateResult updateObjects(const char* ns,
                               const BSONObj& updateobj,
                               const BSONObj& pattern,
                               bool upsert,
                               bool multi,
                               bool logop,
                               OpDebug& debug,
                               bool fromMigrate = false,
                               const QueryPlanSelectionPolicy& planPolicy = QueryPlanSelectionPolicy::any());

}  // namespace mongo
