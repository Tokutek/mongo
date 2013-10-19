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

#include "mongo/db/auth/auth_external_state_s.h"

#include <string>

#include "mongo/base/status.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/jsobj.h"
#include "mongo/s/grid.h"

namespace mongo {

    AuthExternalStateMongos::AuthExternalStateMongos() {}
    AuthExternalStateMongos::~AuthExternalStateMongos() {}

    void AuthExternalStateMongos::startRequest() {
        _checkShouldAllowLocalhost();
    }

    namespace {
        ScopedDbConnection* getConnectionForUsersCollection(const std::string& ns) {
            //
            // Note: The connection mechanism here is *not* ideal, and should not be used elsewhere.
            // If the primary for the collection moves, this approach may throw rather than handle
            // version exceptions.
            //

            DBConfigPtr config = grid.getDBConfig(ns);
            Shard s = config->getShard(ns);

            return ScopedDbConnection::getInternalScopedDbConnection(s.getConnString(), 30.0);
        }
    }

    bool AuthExternalStateMongos::_findUser(const string& usersNamespace,
                                            const BSONObj& query,
                                            BSONObj* result) const {
        scoped_ptr<ScopedDbConnection> conn(getConnectionForUsersCollection(usersNamespace));
        *result = conn->get()->findOne(usersNamespace, query).getOwned();
        conn->done();
        return !result->isEmpty();
    }

} // namespace mongo
