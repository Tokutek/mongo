// client.cpp

/**
*    Copyright (C) 2009 10gen Inc.
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

/* Client represents a connection to the database (the server-side) and corresponds
   to an open socket (or logical connection if pooling on sockets) from a client.
*/

#include "pch.h"

#include "mongo/db/client.h"

namespace mongo {

    // global rw lock used during begin/rollback/commit of multi
    // statement transactions. If a thread wants to examine
    // all of the transactions in the system that are attached to clients,
    // in addition to a GlobalWrite lock, a write lock of this is required.
    // At the momeny (0.1.0), the only thread that needs to examine
    // this information is replication when transitioning from a primary
    // to a secondary. A reason this lock is introduced is that
    // reusing DBRead locks is problematic (due to complications with
    // fileops and capped collections during commit/rollback).
    //
    // This lock must be grabbed before any Global locks or DB locks
    RWLock multiStmtTransactionLock("multiStmtTransaction");

    void Client::TransactionStack::beginTxn(int flags) {
        DEV { LOG(3) << "begin transaction(" << _txns.size() << ") " << flags << endl; }
        TxnContext *currentTxn = (hasLiveTxn()
                                          ? &txn()
                                          : NULL);
        shared_ptr<TxnContext> newTxn(new TxnContext(currentTxn, flags));
        _txns.push(newTxn);
    }

    void Client::TransactionStack::commitTxn(int flags) {
        DEV { LOG(3) << "commit transaction(" << _txns.size() - 1 << ") " << flags << endl; }
        shared_ptr<TxnContext> txn_to_commit = _txns.top();
        txn_to_commit->commit(flags);
        _txns.pop();
    }

    void Client::TransactionStack::commitTxn() {
        int flags = (cmdLine.logFlushPeriod == 0) ? 0 : DB_TXN_NOSYNC;
        commitTxn(flags);
    }

    void Client::TransactionStack::abortTxn() {
        DEV { LOG(3) << "abort transaction(" << _txns.size() - 1 << ")" << endl; }
        _txns.top()->abort();
        _txns.pop();
    }
    uint32_t Client::TransactionStack::numLiveTxns() {
        return _txns.size();
    }
    
    bool Client::TransactionStack::hasLiveTxn() const {
        if (_txns.empty()) {
            return false;
        }
        const TxnContext &currentTxn = txn();
        // We should not be keeping around retired transactions.
        dassert(currentTxn.isLive());
        return currentTxn.isLive();
    }

    TxnContext &Client::TransactionStack::txn() const {
        dassert(!_txns.empty());
        return *(_txns.top());
    }

    Client::Transaction::Transaction(int flags) {
        shared_ptr<TransactionStack> stack = cc()._transactions;
        if (stack == NULL) {
            shared_ptr<TransactionStack> newStack (new TransactionStack());
            stack = newStack;
            cc()._transactions = stack;
        }
        dassert(stack != NULL);
        stack->beginTxn(flags);
        _txn = &stack->txn();
    }

    Client::Transaction::~Transaction() {
        if (_txn == NULL) {
            return;
        }
        Client::TransactionStack *stack = cc()._transactions.get();
        if (stack == NULL) {
            return;
        }
        if (&(stack->txn()) == _txn && _txn->isLive()) {
            abort();
        }
    }

    void Client::Transaction::commit(int flags) {
        dassert(_txn != NULL);
        Client::TransactionStack *stack = cc()._transactions.get();
        dassert(stack != NULL);
        dassert(_txn == &(stack->txn()));
        dassert(_txn->isLive());
        stack->commitTxn(flags);
        _txn = NULL;
    }

    void Client::Transaction::commit() {
        dassert(_txn != NULL);
        Client::TransactionStack *stack = cc()._transactions.get();
        dassert(stack != NULL);
        dassert(_txn == &(stack->txn()));
        dassert(_txn->isLive());
        stack->commitTxn();
        _txn = NULL;
    }

    void Client::Transaction::abort() {
        dassert(_txn != NULL);
        Client::TransactionStack *stack = cc()._transactions.get();
        dassert(stack != NULL);
        dassert(_txn == &stack->txn());
        dassert(_txn->isLive());
        stack->abortTxn();
        _txn = NULL;
    }

    Client::AlternateTransactionStack::AlternateTransactionStack() {
        cc().swapTransactionStack(_saved);
    }
    Client::AlternateTransactionStack::~AlternateTransactionStack() {
        cc().swapTransactionStack(_saved);
    }

} // namespace mongo
