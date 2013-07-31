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
#include "mongo/db/cursor.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/instance.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/storage/exception.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

namespace {
    static Status userNotFoundStatus(ErrorCodes::UserNotFound, "User not found");
}

    AuthzManagerExternalStateMongod::AuthzManagerExternalStateMongod() {}
    AuthzManagerExternalStateMongod::~AuthzManagerExternalStateMongod() {}

    Status AuthzManagerExternalStateMongod::insertPrivilegeDocument(const string& dbname,
                                                                    const BSONObj& userObj) const {
        try {
            string userNS = getSisterNS(dbname, "system.users");
            DBDirectClient client;
            {
                Client::GodScope gs;
                Client::AlternateTransactionStack altStack;
                // TODO(spencer): Once we're no longer fully rebuilding the user cache on every change
                // to user data we should remove the global lock
                LOCK_REASON(lockReason, "auth: inserting privilege document");
                Lock::GlobalWrite w(lockReason);
                client.insert(userNS, userObj);
            }

            // 30 second timeout for w:majority
            BSONObj res = client.getLastErrorDetailed(false, false, -1, 30*1000);
            string errstr = client.getLastErrorString(res);
            if (errstr.empty()) {
                return Status::OK();
            }
            if (res.hasField("code") && res["code"].Int() == ASSERT_ID_DUPKEY) {
                return Status(ErrorCodes::DuplicateKey,
                              mongoutils::str::stream() << "User \"" << userObj["user"].String() <<
                                     "\" already exists on database \"" << dbname << "\"");
            }
            return Status(ErrorCodes::UserModificationFailed, errstr);
        } catch (const DBException& e) {
            return e.toStatus();
        }
    }

    Status AuthzManagerExternalStateMongod::updatePrivilegeDocument(
            const UserName& user, const BSONObj& updateObj) const {
        try {
            string userNS = getSisterNS(user.getDB(), "system.users");
            DBDirectClient client;
            {
                Client::GodScope gs;
                Client::AlternateTransactionStack altStack;
                // TODO(spencer): Once we're no longer fully rebuilding the user cache on every change
                // to user data we should remove the global lock
                LOCK_REASON(lockReason, "auth: updating privilege document");
                Lock::GlobalWrite w(lockReason);
                client.update(userNS,
                              QUERY("user" << user.getUser() << "userSource" << BSONNULL),
                              updateObj);
            }

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
        } catch (const DBException& e) {
            return e.toStatus();
        }
    }

    Status AuthzManagerExternalStateMongod::removePrivilegeDocuments(const string& dbname,
                                                                     const BSONObj& query) const {
        try {
            string userNS = getSisterNS(dbname, "system.users");
            DBDirectClient client;
            {
                Client::GodScope gs;
                // TODO(spencer): Once we're no longer fully rebuilding the user cache on every
                // change to user data we should remove the global lock and uncomment the
                // WriteContext below
                LOCK_REASON(lockReason, "auth: removing privilege document");
                Lock::GlobalWrite w(lockReason);
                // Client::WriteContext ctx(userNS);
                client.remove(userNS, query);
            }

            // 30 second timeout for w:majority
            BSONObj res = client.getLastErrorDetailed(false, false, -1, 30*1000);
            string errstr = client.getLastErrorString(res);
            if (!errstr.empty()) {
                return Status(ErrorCodes::UserModificationFailed, errstr);
            }

            int numUpdated = res["n"].numberInt();
            if (numUpdated == 0) {
                return Status(ErrorCodes::UserNotFound,
                              mongoutils::str::stream() << "No users found on database \"" << dbname
                                      << "\" matching query: " << query.toString());
            }
            return Status::OK();
        } catch (const DBException& e) {
            return e.toStatus();
        }
    }

    Status AuthzManagerExternalStateMongod::_findUser(const string& usersNamespace,
                                                      const BSONObj& query,
                                                      BSONObj* result) const {
        try {
            Client::GodScope gs;
            LOCK_REASON(lockReason, "auth: looking up user");
            Client::ReadContext ctx(usersNamespace, lockReason);
            // we want all authentication stuff to happen on an alternate stack
            Client::AlternateTransactionStack altStack;
            Client::Transaction txn(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
            BSONObj tmpresult;
            if (!Collection::findOne(usersNamespace, query, result != NULL ? *result : tmpresult)) {
                return userNotFoundStatus;
            }
            txn.commit();
            return Status::OK();
        } catch (storage::LockException &e) {
            LOG(1) << "Couldn't read from system.users because of " << e.what() << ", assuming it's empty." << endl;
            return userNotFoundStatus;
        }
    }

    Status AuthzManagerExternalStateMongod::getAllDatabaseNames(
            std::vector<std::string>* dbnames) const {
        LOCK_REASON(lockReason, "auth: getting database names");
        Lock::GlobalRead lk(lockReason);
        Client::AlternateTransactionStack altStack;
        Client::Transaction txn(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
        getDatabaseNames(*dbnames);
        txn.commit();
        return Status::OK();
    }

    Status AuthzManagerExternalStateMongod::getAllV1PrivilegeDocsForDB(
            const std::string& dbname, std::vector<BSONObj>* privDocs) const {
        try {
            Client::GodScope gs;
            LOCK_REASON(lockReason, "auth: looking up users");
            Client::ReadContext ctx(dbname, lockReason);

            Client::AlternateTransactionStack altStack;
            Client::Transaction txn(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);

            std::string collname = getSisterNS(dbname, "system.users");
            Collection *c = getCollection(collname);
            if (!c) {
                LOG(1) << "No " << collname << " found to look up privilege docs." << endl;
                return Status::OK();
            }

            for (shared_ptr<Cursor> cur(Cursor::make(c)); cur->ok(); cur->advance()) {
                if (cur->currentMatches()) {
                    privDocs->push_back(cur->current().getOwned());
                }
            }

            txn.commit();
        } catch (storage::LockException &e) {
            LOG(1) << "Couldn't read from system.users because of " << e.what() << ", assuming it's empty." << endl;
        }

        return Status::OK();
    }

} // namespace mongo
