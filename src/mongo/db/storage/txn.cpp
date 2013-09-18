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

#include "txn.h"

#include "mongo/pch.h"

#include <db.h>

#include "mongo/db/client.h"
#include "mongo/db/storage/env.h"

namespace mongo {

    namespace storage {

        static DB_TXN *start_txn(DB_TXN *parent, int flags) {
            DB_TXN *db_txn;
            int r = env->txn_begin(env, parent, &db_txn, flags);
            if (r != 0) {
                handle_ydb_error(r);
            }
            return db_txn;
        }

        static void commit_txn(DB_TXN *db_txn, int flags) {
            int r = db_txn->commit(db_txn, flags);
            if (r != 0) {
                handle_ydb_error(r);
            }
        }

        static void abort_txn(DB_TXN *db_txn) {
            int r = db_txn->abort(db_txn);
            if (r != 0) {
                handle_ydb_error(r);
            }
        }

        Txn::Txn(const Txn *parent, int flags)
                : _db_txn(start_txn((parent == NULL
                                     ? NULL
                                     : parent->_db_txn),
                                    (parent == NULL
                                     ? flags
                                     : DB_INHERIT_ISOLATION))),
                 _flags(parent == NULL
                        ? flags
                        : parent->_flags)
        {}

        Txn::~Txn() {
            if (isLive()) {
                abort();
            }
        }

        void Txn::commit(int flags) {
            dassert(isLive());
            storage::commit_txn(_db_txn, flags);
            _db_txn = NULL;
        }

        void Txn::abort() {
            dassert(isLive());
            storage::abort_txn(_db_txn);
            _db_txn = NULL;
        }

    } // namespace storage

} // namespace mongo
