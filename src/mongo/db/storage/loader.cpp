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

#include "mongo/db/curop.h"
#include "mongo/db/interrupt_status.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/loader.h"

namespace mongo {

    namespace storage {

        Loader::Loader(DB *db) :
            _db(db), _loader(NULL),
            _poll_extra(cc()), _closed(false) {

            uint32_t db_flags = 0;
            uint32_t dbt_flags = 0;
            // TODO: Use a command line option for LOADER_COMPRESS_INTERMEDIATES
            const int loader_flags = 0; 
            int r = storage::env->create_loader(storage::env, cc().txn().db_txn(),
                                                &_loader, _db, 1, &_db,
                                                &db_flags, &dbt_flags, loader_flags);
            if (r != 0) {
                handle_ydb_error(r);
            }
            r = _loader->set_poll_function(_loader, poll_function, &_poll_extra);
            if (r != 0) {
                handle_ydb_error(r);
            }
        }

        Loader::~Loader() {
            if (!_closed && _loader != NULL) {
                int r = _loader->abort(_loader);
                if (r != 0) {
                    problem() << "storage::~Loader, failed to close DB_LOADER, error: "
                              << r << endl;
                }
            }
        }

        int Loader::poll_function(void *extra, float progress) {
            poll_function_extra *info = static_cast<poll_function_extra *>(extra);
            try {
                killCurrentOp.checkForInterrupt(info->c); // uasserts if we should stop
                return 0;
            }
            catch (const std::exception &ex) {
                info->saveException(ex);
                return -1;
            }
        }

        int Loader::put(DBT *key, DBT *val) {
            return _loader->put(_loader, key, val);
        }

        int Loader::close() {
            const int r = _loader->close(_loader);
            if (r == -1) {
                _poll_extra.throwException();
            }
            if (r == 0) {
                _closed = true;
            }
            return r;
        }

    } // namespace storage

} // namespace mongo
