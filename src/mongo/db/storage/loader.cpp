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

#include <db.h>

#include "mongo/db/curop.h"
#include "mongo/db/interrupt_status.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/storage_options.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/builder.h"

namespace mongo {

    namespace storage {

        // TODO: Use a command line option for LOADER_COMPRESS_INTERMEDIATES

        Loader::Loader(DB **dbs, const int n, const std::string &prefix)
                : BuilderBase(prefix),
                  _dbs(dbs), _n(n),
                  _loader(NULL), _closed(false) {

            uint32_t db_flags = 0;
            uint32_t dbt_flags = 0;
            const int loader_flags = (storageGlobalParams.loaderCompressTmp
                                      ? LOADER_COMPRESS_INTERMEDIATES
                                      : 0);
            int r = storage::env->create_loader(storage::env, cc().txn().db_txn(),
                                                &_loader, _dbs[0], _n, _dbs,
                                                &db_flags, &dbt_flags, loader_flags);
            if (r != 0) {
                handle_ydb_error(r);
            }
            r = _loader->set_poll_function(_loader, BuilderBase::poll_function, &_poll_extra);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            r = _loader->set_error_callback(_loader, BuilderBase::error_callback, &_error_extra);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
        }

        Loader::~Loader() {
            if (!_closed && _loader != NULL) {
                const int r = _loader->abort(_loader);
                if (r != 0) {
                    problem() << "storage::~Loader, failed to close DB_LOADER, error: "
                              << r << endl;
                }
            }
        }

        int Loader::put(DBT *key, DBT *val) {
            return _loader->put(_loader, key, val);
        }

        int Loader::close() {
            const int r = _loader->close(_loader);

            // Doesn't matter if the close succeeded or not. It's dead to us now.
            _closed = true;
            if (r == -1) {
                _poll_extra.throwException();
            }
            // Forward any callback-generated exception. Could be an error
            // from the error callback or an interrupt from the poll function.
            uassert( 16860, _error_extra.errmsg, _error_extra.errmsg.empty() );
            // Any other non-zero error code that didn't trigger the error
            // callback or come from the poll function should be handled
            // in some generic fashion by the caller.
            return r;
        }

    } // namespace storage

    ExportedServerParameter<bool> LoaderCompressTmpSetting(ServerParameterSet::getGlobal(),
                                                           "loaderCompressTmp",
                                                           &storageGlobalParams.loaderCompressTmp,
                                                           true,
                                                           true);

} // namespace mongo
