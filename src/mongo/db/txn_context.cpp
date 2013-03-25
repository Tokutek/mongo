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

#include "txn_context.h"

#include "mongo/pch.h"

#include <db.h>
#include "mongo/db/storage/env.h"
#include "mongo/bson/bsonobjiterator.h"
#include "mongo/db/oplog.h"
#include "mongo/db/repl.h"

namespace mongo {

    // making a static bool to tell us whether logging of operations is on
    // because this bool depends on replication, and replication is not
    // compiled with coredb. So, in startReplication, we will set this
    // to true
    static bool logTxnOperations = false;
    static void (*_logTxnToOplog)(BSONArray& opInfo) = NULL;

    void setTxnLogOperations(bool val) {
        logTxnOperations = val;
    }

    void setLogTxnToOplog(void (*f)(BSONArray& opInfo)) {
        _logTxnToOplog = f;
    }

    TxnContext::TxnContext(TxnContext *parent, int txnFlags)
            : _txn((parent == NULL) ? NULL : &parent->_txn, txnFlags), 
              _parent(parent),
              _numOperations(0)
    {
    }

    TxnContext::~TxnContext() {
    }

    void TxnContext::commit(int flags) {
        _txn.commit(flags);
    }

    void TxnContext::abort() {
        _txn.abort();
    }

    void TxnContext::logOp(BSONObj op)
    {
        if (logTxnOperations) {
            _txnOps.append(op);
            _numOperations++;
        }
    }

    bool TxnContext::hasParent() {
        return (_parent != NULL);
    }

    void TxnContext::transferOpsToParent() {
        BSONArray array = _txnOps.arr();
        BSONObjIterator iter(array);
        while (iter.more()) {
            BSONElement curr = iter.next();
            _parent->logOp(curr.Obj());
        }
    }

    void TxnContext::writeOpsToOplog() {
        if (_numOperations > 0) {
            dassert(logTxnOperations);
            dassert(_logTxnToOplog);
            BSONArray array = _txnOps.arr();        
            _logTxnToOplog(array);
        }
    }
} // namespace mongo
