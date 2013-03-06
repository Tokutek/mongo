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

        DB_TXN *start_txn(DB_TXN *parent, int flags) {
            DB_TXN *txn;
            int r = env->txn_begin(env, parent, &txn, flags);
            verify(r == 0);
            return txn;
        }

        void commit_txn(DB_TXN *txn, int flags) {
            // TODO: move to only where we need it
            const int extra_flags = (cmdLine.logFlushPeriod == 0) ? 0 : DB_TXN_NOSYNC;
            int r = txn->commit(txn, flags | extra_flags);
            verify(r == 0);
        }

        void abort_txn(DB_TXN *txn) {
            int r = txn->abort(txn);
            verify(r == 0);
        }

    } // namespace storage

    Client::Context::Transaction::Transaction(const Client::Context::Transaction *parent, int flags) {
        DB_TXN *parent_txn = (parent != NULL ? parent->_txn : NULL);
        _txn = storage::start_txn(parent_txn, flags);
    }

    Client::Context::Transaction::~Transaction() {
        if (_txn) {
            abort();
        }
    }

    void Client::Context::Transaction::commit(int flags) {
        dassert(_txn);
        storage::commit_txn(_txn, flags);
        _txn = NULL;
    }

    void Client::Context::Transaction::abort() {
        dassert(_txn);
        storage::abort_txn(_txn);
        _txn = NULL;
    }

    const Client::Context::Transaction &Client::Context::transaction() const {
        if (_transaction.get() != NULL) {
            return *_transaction;
        } else {
            dassert(_oldContext != NULL);
            return _oldContext->transaction();
        }
    }

    bool Client::Context::hasTransaction() const {
        return ((_transaction.get() != NULL) ||
                (_oldContext != NULL && _oldContext->hasTransaction()));
    }

    bool Client::Context::transactionIsRoot() const {
        if (_transaction.get() != NULL) {
            return (_oldContext == NULL || !_oldContext->hasTransaction());
        } else {
            dassert(_oldContext != NULL);
            return _oldContext->transactionIsRoot();
        }
    }

    void Client::Context::beginTransaction(int flags) {
        dassert(_transaction.get() == NULL);
        const Transaction *parent = NULL;
        if (hasTransaction()) {
            parent = &_oldContext->transaction();
        }
        const int safe_flags = ((_oldContext == NULL || !_oldContext->hasTransaction())
                                ? flags
                                : DB_INHERIT_ISOLATION);
        _transaction.reset(new Transaction(parent, safe_flags));
    }

    void Client::Context::commitTransaction(int flags) {
        dassert(_transaction.get() != NULL);
        _transaction->commit(flags);
        _transaction.reset();
    }

    void Client::Context::swapTransactions(shared_ptr<Transaction> &other) {
        if (_transaction.get() != NULL) {
            dassert(transactionIsRoot());
        }
        _transaction.swap(other);
    }

} // namespace mongo
