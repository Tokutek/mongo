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
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    RemoteTransaction::RemoteTransaction(DBClientWithCommands &conn, const string &isolation) : _conn(NULL) {
        BSONObj res;
        bool ok = conn.beginTransaction(isolation, &res);
        if (ok) {
            _conn = &conn;
        } else {
            LOG(0) << "error in beginTransaction: " << res << endl;
        }
    }

    RemoteTransaction::~RemoteTransaction() {
        try {
            rollback();
        }
        catch (DBException &e) {
            LOG(1) << "error rolling back RemoteTransaction" << endl;
            // not much else we can do
        }
    }

    bool RemoteTransaction::commit(BSONObj *res) {
        if (!_conn) {
            *res = BSON("ok" << 0 <<
                        "errmsg" << "no live transaction to commit");
            return false;
        }
        bool ok = _conn->commitTransaction(res);
        if (ok) {
            _conn = NULL;
        }
        return ok;
    }

    bool RemoteTransaction::rollback(BSONObj *res) {
        if (!_conn) {
            *res = BSON("ok" << 1 <<
                        "errmsg" << "no live transaction to abort");
            return true;
        }
        bool ok = _conn->rollbackTransaction(res);
        if (ok) {
            _conn = NULL;
        }
        return ok;
    }

} // namespace mongo
