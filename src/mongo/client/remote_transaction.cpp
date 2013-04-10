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

    RemoteTransaction::RemoteTransaction(shared_ptr<ScopedDbConnection> &conn, const string &isolation) : _conn(conn) {
        bool ok;
        try {
            BSONObj beginResult;
            ok = _conn->get()->runCommand("", BSON( "beginTransaction" << "" << "isolation" << isolation ), beginResult);
        }
        catch (DBException &e) {
            ok = false;
        }
        verify(ok);
    }

    RemoteTransaction::~RemoteTransaction() {
        if (_conn) {
            rollback();
        }
    }

    void RemoteTransaction::commit() {
        bool ok;
        try {
            BSONObj commitResult;
            ok = _conn->get()->runCommand("", BSON( "commitTransaction" << "" ), commitResult);
        }
        catch (DBException &e) {
            ok = false;
        }
        verify(ok);
        _conn.reset();
    }

    void RemoteTransaction::rollback() {
        bool ok;
        try {
            BSONObj rollbackResult;
            ok = _conn->get()->runCommand("", BSON( "rollbackTransaction" << "" ), rollbackResult);
        }
        catch (DBException &e) {
            ok = false;
        }
        verify(ok);
        _conn.reset();
    }

} // namespace mongo
