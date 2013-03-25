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
#include "mongo/db/gtid.h"

namespace mongo {

    // making a static bool to tell us whether logging of operations is on
    // because this bool depends on replication, and replication is not
    // compiled with coredb. So, in startReplication, we will set this
    // to true
    static bool logTxnOperations = false;
    static void (*_logTxnToOplog)(BSONObj id, BSONArray& opInfo) = NULL;
    static GTIDManager* txnGTIDManager = NULL;

    void setTxnLogOperations(bool val) {
        logTxnOperations = val;
    }

    void setLogTxnToOplog(void (*f)(BSONObj id, BSONArray& opInfo)) {
        _logTxnToOplog = f;
    }

    void setTxnGTIDManager(GTIDManager* m) {
        txnGTIDManager = m;
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
        bool gotGTID = false;
        GTID gtid;
        // handle work related to logging of transaction for replication
        if (_numOperations > 0) {
            if (hasParent()) {
                // In this case, what transaction we are committing has a parent
                // and therefore we must transfer the opLog information from 
                // this transaction to the parent
                transferOpsToParent();
            }
            else {
                dassert(txnGTIDManager);
                gtid = txnGTIDManager->getGTID();
                gotGTID = true;
                // In this case, the transaction we are committing has
                // no parent, so we must write the transaction's 
                // logged operations to the opLog, as part of this transaction
                writeOpsToOplog(gtid);
            }
        }
        _txn.commit(flags);
        // if the commit of this transaction got a GTID, then notify 
        // the GTIDManager that the commit is now done.
        if (gotGTID) {
            dassert(txnGTIDManager);
            txnGTIDManager->noteGTIDDone(gtid);
        }
    }

    void TxnContext::abort() {
        _txn.abort();
    }

    void TxnContext::logOp(BSONObj op) {
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

    void TxnContext::writeOpsToOplog(GTID gtid) {
        dassert(logTxnOperations);
        dassert(_logTxnToOplog);
        BSONArray array = _txnOps.arr();        
        _logTxnToOplog(gtid.getBSON(), array);
    }
} // namespace mongo
