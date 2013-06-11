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

#include "mongo/db/auth/authz_manager_external_state_d.h"

#include "mongo/base/status.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/client.h"
#include "mongo/db/collection.h"
#include "mongo/db/instance.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/storage/exception.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    AuthzManagerExternalStateMongod::AuthzManagerExternalStateMongod() {}
    AuthzManagerExternalStateMongod::~AuthzManagerExternalStateMongod() {}

    Status AuthzManagerExternalStateMongod::insertPrivilegeDocument(const string& dbname,
                                                                    const BSONObj& userObj) const {
        string userNS = getSisterNS(dbname, "system.users");
        Client::GodScope gs;
        Client::AlternateTransactionStack altStack;

        DBDirectClient client;
        client.insert(userNS, userObj);

        // 30 second timeout for w:majority
        BSONObj res = client.getLastErrorDetailed(false, false, -1, 30*1000);
        string errstr = client.getLastErrorString(res);
        if (errstr.empty()) {
            return Status::OK();
        }
        if (res["code"].Int() == ASSERT_ID_DUPKEY) {
            return Status(ErrorCodes::DuplicateKey,
                          mongoutils::str::stream() << "User \"" << userObj["user"].String() <<
                                 "\" already exists on database \"" << dbname << "\"");
        }
        return Status(ErrorCodes::UserModificationFailed, errstr);
    }

    Status AuthzManagerExternalStateMongod::updatePrivilegeDocument(
            const UserName& user, const BSONObj& updateObj) const {
        string userNS = mongoutils::str::stream() << user.getDB() << ".system.users";
        Client::GodScope gs;
        Client::WriteContext ctx(userNS);

        DBDirectClient client;
        client.update(userNS,
                      QUERY("user" << user.getUser() << "userSource" << BSONNULL),
                      updateObj);

        // 30 second timeout for w:majority
        BSONObj res = client.getLastErrorDetailed(false, false, -1, 30*1000);
        string err = client.getLastErrorString(res);
        if (!err.empty()) {
            return Status(ErrorCodes::UserModificationFailed, err);
        }

        int numUpdated = res["n"].numberInt();
        dassert(numUpdated <= 1 && numUpdated >= 0);
        if (numUpdated == 0) {
            return Status(ErrorCodes::UserNotFound,
                          mongoutils::str::stream() << "User " << user.getFullName() <<
                                  " not found");
        }

        return Status::OK();
    }

    bool AuthzManagerExternalStateMongod::_findUser(const string& usersNamespace,
                                                    const BSONObj& query,
                                                    BSONObj* result) const {
        bool ok = false;
        try {
            Client::GodScope gs;
            LOCK_REASON(lockReason, "auth: looking up user");
            Client::ReadContext ctx(usersNamespace, lockReason);
            // we want all authentication stuff to happen on an alternate stack
            Client::AlternateTransactionStack altStack;
            Client::Transaction txn(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
            BSONObj tmpresult;
            ok = Collection::findOne(usersNamespace, query, result != NULL ? *result : tmpresult);
            if (ok) {
                txn.commit();
            }
        } catch (storage::LockException &e) {
            LOG(1) << "Couldn't read from system.users because of " << e.what() << ", assuming it's empty." << endl;
        }
        return ok;
    }

} // namespace mongo
