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
#include "mongo/db/storage/env.h"

namespace mongo {

    namespace storage {

        Transaction::Transaction() : _parent(cc().transaction()), _retired(false) {
            int r = env->txn_begin(env, _parent->txn(), &_txn, 0);
            verify(r == 0);
            cc().set_transaction(this);
        }

        Transaction::~Transaction() {
            if (!_retired) {
                abort();
            }
        }

        void Transaction::commit() {
            verify(cc().transaction() == this);
            int r = _txn->commit(_txn, 0);
            verify(r == 0);
            cc().set_transaction(_parent);
            _retired = true;
        }

        void Transaction::abort() {
            verify(cc().transaction() == this);
            int r = _txn->abort(_txn);
            verify(r == 0);
            cc().set_transaction(_parent);
            _retired = true;
        }


    } // namespace storage

} // namespace mongo
