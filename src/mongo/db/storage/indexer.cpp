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

#include "mongo/db/curop.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/builder.h"

namespace mongo {

    namespace storage {

        Indexer::Indexer(DB *src_db, DB *dest_db, const std::string &prefix)
                : BuilderBase(prefix),
                  _dest_db(dest_db), _indexer(NULL), _closed(false) {
            uint32_t db_flags = 0;
            uint32_t indexer_flags = 0;
            DB_ENV *env = storage::env;
            int r = env->create_indexer(env, cc().txn().db_txn(), &_indexer,
                                        src_db, 1, &_dest_db,
                                        &db_flags, indexer_flags);
            if (r != 0) {
                storage::handle_ydb_error(r);
            }
            r = _indexer->set_poll_function(_indexer, BuilderBase::poll_function, &_poll_extra);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            r = _indexer->set_error_callback(_indexer, BuilderBase::error_callback, &_error_extra);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
        }

        Indexer::~Indexer() {
            if (!_closed && _indexer != NULL) {
                const int r = _indexer->abort(_indexer);
                if (r != 0) {
                    problem() << "storage::~Indexer, failed to close DB_INDEXER, error: "
                              << r << endl;
                }
            }
        }

        int Indexer::build() {
            const int r = _indexer->build(_indexer);
            if (r == -1) {
                _poll_extra.throwException();
            }
            // Forward any callback-generated exception. Could be an error
            // from the error callback or an interrupt from the poll function.
            uassert( 16911, _error_extra.errmsg, _error_extra.errmsg.empty() );
            // Any other non-zero error code that didn't trigger the error
            // callback or come from the poll function should be handled
            // in some generic fashion by the caller.
            return r;
        }

        int Indexer::close() {
            // Doesn't matter if the close succeeds or not.
            _closed = true;
            return _indexer->close(_indexer);
        }

    } // namespace storage

} // namespace mongo



