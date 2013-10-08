/**
*    Copyright (C) 2012 10gen Inc.
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

#include "mongo/db/auth/auth_external_state_d.h"

#include "mongo/base/status.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/client.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/instance.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/storage/exception.h"

namespace mongo {

    AuthExternalStateMongod::AuthExternalStateMongod() {}
    AuthExternalStateMongod::~AuthExternalStateMongod() {}

    void AuthExternalStateMongod::startRequest() {
        if (!Lock::isLocked()) {
            _checkShouldAllowLocalhost();
        }
    }

    bool AuthExternalStateMongod::_findUser(const string& usersNamespace,
                                            const BSONObj& query,
                                            BSONObj* result) const {
        bool ok = false;
        try {
            Client::GodScope gs;
            Client::ReadContext ctx(usersNamespace);
            // we want all authentication stuff to happen on an alternate stack
            Client::AlternateTransactionStack altStack;

            Client::Transaction txn(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
            NamespaceDetails *d = nsdetails(usersNamespace);
            if (d == NULL) {
                return false;
            }
            BSONObj tmpResult;
            ok = d->findOne(query, result != NULL ? *result : tmpResult);
            if (ok) {
                txn.commit();
            }
        } catch (storage::LockException &e) {
            LOG(1) << "Couldn't read from system.users because of " << e.what() << ", assuming it's empty.";
        }
        return ok;
    }

    bool AuthExternalStateMongod::shouldIgnoreAuthChecks() const {
        return cc().isGod() || AuthExternalStateServerCommon::shouldIgnoreAuthChecks();
    }

} // namespace mongo
