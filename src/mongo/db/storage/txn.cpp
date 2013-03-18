/**
*    Copyright (C) 2012 Tokutek Inc.
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
#include "mongo/db/cmdline.h"
#include "mongo/db/storage/env.h"

namespace mongo {

    namespace storage {

        static DB_TXN *start_txn(DB_TXN *parent, int flags) {
            DB_TXN *db_txn;
            int r = env->txn_begin(env, parent, &db_txn, flags);
            verify(r == 0);
            return db_txn;
        }

        static void commit_txn(DB_TXN *db_txn, int flags) {
            // TODO: move to only where we need it
            const int extra_flags = (cmdLine.logFlushPeriod == 0) ? 0 : DB_TXN_NOSYNC;
            int r = db_txn->commit(db_txn, flags | extra_flags);
            verify(r == 0);
        }

        static void abort_txn(DB_TXN *db_txn) {
            int r = db_txn->abort(db_txn);
            verify(r == 0);
        }

        Txn::Txn(const Txn *parent, int flags)
                : _flags(parent == NULL
                         ? flags
                         : parent->_flags),
                  _db_txn(start_txn((parent == NULL
                                     ? NULL
                                     : parent->_db_txn),
                                    _flags))
        {
            DEV { LOG(3) << "begin txn " << _db_txn << " (" << (parent == NULL ? NULL : parent->_db_txn) << ", " << _flags << ")" << endl; }
        }

        Txn::~Txn() {
            if (isLive()) {
                abort();
            }
        }

        void Txn::commit(int flags) {
            dassert(isLive());
            DEV { LOG(3) << "commit txn " << _db_txn << " with flags " << flags << endl; }
            storage::commit_txn(_db_txn, flags);
            _db_txn = NULL;
        }

        void Txn::abort() {
            dassert(isLive());
            DEV { LOG(3) << "abort txn " << _db_txn << endl; }
            storage::abort_txn(_db_txn);
            _db_txn = NULL;
        }

        bool Txn::isReadOnly() const {
            return _flags & DB_TXN_READ_ONLY;
        }

    } // namespace storage

} // namespace mongo
