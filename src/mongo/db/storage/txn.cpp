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

#include "mongo/db/storage/env.h"

namespace mongo {

    namespace storage {

        DB_TXN *start_txn(DB_TXN *parent, int flags) {
            DB_TXN *txn;
            int r = env->txn_begin(env, parent, &txn, flags);
            verify(r == 0);
            return txn;
        }

        void commit_txn(DB_TXN *txn) {
            int r = txn->commit(txn, 0);
            verify(r == 0);
        }

        void abort_txn(DB_TXN *txn) {
            int r = txn->abort(txn);
            verify(r == 0);
        }

    } // namespace storage

} // namespace mongo
