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

#ifndef MONGO_DB_STORAGE_CURSOR_H
#define MONGO_DB_STORAGE_CURSOR_H

#include "mongo/pch.h"
#include "mongo/db/client.h"

#include <db.h>

namespace mongo {

    namespace storage {

        // RAII wrapper for a TokuDB DBC
        class Cursor {
        public:
            Cursor(DB *db, const int flags = 0);
            ~Cursor();
            DBC *dbc() const {
                return _dbc;
            }
        protected:
            DBC *_dbc;
        };

        class DirectoryCursor : public Cursor {
        public:
            DirectoryCursor(DB_ENV *env, DB_TXN *txn);
        };


    } // namespace storage

} // namespace mongo

#endif // MONGO_DB_STORAGE_CURSOR_H

