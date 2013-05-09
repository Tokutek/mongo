/** @file remote_transaction.cpp */

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

#include "remote_transaction.h"

#include "mongo/pch.h"

#include "mongo/client/connpool.h"
#include "mongo/db/jsobj.h"
#include "mongo/util/assert_util.h"

namespace mongo {

    RemoteTransaction::RemoteTransaction(DBClientWithCommands &conn, const string &isolation) : _conn(&conn) {
        bool ok = _conn->beginTransaction(isolation);
        verify(ok);
    }

    RemoteTransaction::~RemoteTransaction() {
        if (_conn) {
            bool ok;
            try {
                ok = rollback();
            }
            catch (DBException &e) {
                ok = false;
            }
            verify(ok);
        }
    }

    bool RemoteTransaction::commit(BSONObj *res) {
        bool ok = _conn->commitTransaction(res);
        if (ok) {
            _conn = NULL;
        }
        return ok;
    }

    bool RemoteTransaction::rollback(BSONObj *res) {
        bool ok = _conn->rollbackTransaction(res);
        if (ok) {
            _conn = NULL;
        }
        return ok;
    }

} // namespace mongo
