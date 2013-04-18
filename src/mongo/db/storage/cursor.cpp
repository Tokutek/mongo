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

#include "mongo/pch.h"

#include "mongo/db/storage/env.h"
#include "mongo/db/storage/cursor.h"

namespace mongo {

    namespace storage {

        Cursor::Cursor(DB *db, const int flags) : _dbc(NULL) {
            if (db != NULL) {
                int r = db->cursor(db, cc().txn().db_txn(), &_dbc, flags);
                if (r != 0) {
                    handle_ydb_error(r);
                }
            }
        }

        Cursor::~Cursor() {
            if (_dbc != NULL) {
                int r = _dbc->c_close(_dbc);
                if (r != 0) {
                    handle_ydb_error(r);
                }
            }
        }

    } // namespace storage

} // namespace mongo
