// security.cpp

/**
 *    Copyright (C) 2009 10gen Inc.
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

#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/security.h"
#include "mongo/db/security_common.h"
#include "mongo/db/instance.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/client.h"
#include "mongo/db/curop.h"

// this is the _mongod only_ implementation of security.h

namespace mongo {

    bool CmdLogout::run(const string& dbname , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
        AuthorizationManager* authManager = cc().getAuthorizationManager();
        authManager->logoutDatabase(dbname);
        return true;
    }

} // namespace mongo
