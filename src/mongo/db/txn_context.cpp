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


#include "mongo/pch.h"

#include "mongo/bson/bsonobjiterator.h"
#include "mongo/db/oplog.h"
#include "mongo/db/repl.h"
#include "mongo/db/gtid.h"
#include "mongo/db/txn_context.h"
#include "mongo/db/storage/env.h"

namespace mongo {

    // making a static bool to tell us whether logging of operations is on
    // because this bool depends on replication, and replication is not
    // compiled with coredb. So, in startReplication, we will set this
    // to true
    static bool _logTxnOperations = false;
    static void (*_logTxnToOplog)(GTID gtid, BSONArray& opInfo) = NULL;
    static GTIDManager* txnGTIDManager = NULL;
    // TODO: Remove this function pointer, replace it with more sane linking.
    //
    // It's a big mess linking txn_context.o and namespace_details.o in all
    // the right places (coredb? serveronly? mongos? something always breaks)
    static void (*_noteTxnCompleted)(const string &ns,
                                     const vector<BSONObj> &insertedPKs,
                                     long long nDelta,
                                     long long sizeDelta,
                                     bool committed);

    void setTxnLogOperations(bool val) {
        _logTxnOperations = val;
    }

    bool logTxnOperations() {
        return _logTxnOperations;
    }

    void setLogTxnToOplog(void (*f)(GTID gtid, BSONArray& opInfo)) {
        _logTxnToOplog = f;
    }

    void setNoteTxnCompleted(void (*f)(const string &ns,
                                       const vector<BSONObj> &insertedPKs,
                                       long long nDelta,
                                       long long sizeDelta,
                                       bool committed)) {
        _noteTxnCompleted = f;
    }

    void setTxnGTIDManager(GTIDManager* m) {
        txnGTIDManager = m;
    }

    TxnContext::TxnContext(TxnContext *parent, int txnFlags)
            : _txn((parent == NULL) ? NULL : &parent->_txn, txnFlags), 
              _parent(parent),
              _retired(false),
              _numOperations(0),
              _initiatingRS(false)
    {
    }

    TxnContext::~TxnContext() {
        if (!_retired) {
            abort();
        }
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
                if (!_initiatingRS) {
                    dassert(txnGTIDManager);
                    gtid = txnGTIDManager->getGTIDForPrimary();
                }
                else {
                    dassert(!txnGTIDManager);
                }
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
        if (gotGTID && !_initiatingRS) {
            dassert(txnGTIDManager);
            txnGTIDManager->noteLiveGTIDDone(gtid);
        }

        if (hasParent()) {
            _cappedRollback.transfer(_parent->_cappedRollback);
        } else {
            _cappedRollback.commit();
        }
        _retired = true;
    }

    void TxnContext::abort() {
        _txn.abort();
        _cappedRollback.abort();
        _retired = true;
    }

    void TxnContext::logOp(BSONObj op) {
        if (_logTxnOperations) {
            _txnOps.append(op);
            _numOperations++;
        }
    }

    bool TxnContext::hasParent() {
        return (_parent != NULL);
    }

    void TxnContext::txnIntiatingRs() {
        _initiatingRS = true;
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
        dassert(_logTxnOperations);
        dassert(_logTxnToOplog);
        BSONArray array = _txnOps.arr();
        _logTxnToOplog(gtid, array);
    }

    void CappedCollectionRollback::_complete(const bool committed) {
        for (ContextMap::const_iterator it = _map.begin(); it != _map.end(); it++) {
            const string &ns = it->first;
            const Context &c = it->second;
            _noteTxnCompleted(ns, c.insertedPKs, c.nDelta, c.sizeDelta, committed);
        }
    }

    void CappedCollectionRollback::commit() {
        _complete(true);
    }

    void CappedCollectionRollback::abort() {
        _complete(false);
    }

    void CappedCollectionRollback::transfer(CappedCollectionRollback &parent) {
        for (ContextMap::const_iterator it = _map.begin(); it != _map.end(); it++) {
            const string &ns = it->first;
            const Context &c = it->second;
            Context &parentContext = parent._map[ns];
            vector<BSONObj> &pks = parentContext.insertedPKs;
            pks.insert(pks.end(), c.insertedPKs.begin(), c.insertedPKs.end());
            parentContext.nDelta += c.nDelta;
            parentContext.sizeDelta += c.sizeDelta;
        }
    }

    void CappedCollectionRollback::noteInsert(const string &ns, const BSONObj &pk, long long size) {
        Context &c = _map[ns];
        c.insertedPKs.push_back(pk.getOwned());
        c.nDelta++;
        c.sizeDelta += size;
    }

    void CappedCollectionRollback::noteDelete(const string &ns, const BSONObj &pk, long long size) {
        Context &c = _map[ns];
        c.nDelta--;
        c.sizeDelta -= size;
    }

} // namespace mongo
