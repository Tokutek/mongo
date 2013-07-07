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

#pragma once

#include "mongo/pch.h"
#include "mongo/db/client.h"

#include <db.h>

namespace mongo {

    namespace storage {

        // RAII wrapper for a DB_LOADER.
        class Loader {
        public:

            Loader(DB *db);

            ~Loader();

            int put(DBT *key, DBT *val);

            int close();

            struct poll_function_extra {
                poll_function_extra(Client &client) : c(client), ex(NULL) { }
                Client &c;
                std::exception *ex;
            };
            static int poll_function(void *extra, float progress);
            struct error_callback_extra {
                error_callback_extra() { }
                string errmsg;
            };
            static void error_callback(DB *db, int i, int err,
                                       DBT *key, DBT *val, void *extra);

        private:
            DB *_db;
            DB_LOADER *_loader;
            poll_function_extra _poll_extra;
            error_callback_extra _error_extra;
            bool _closed;
        };

    } // namespace storage

} // namespace mongo

