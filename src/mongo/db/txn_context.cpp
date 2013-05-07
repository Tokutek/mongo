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
#include "mongo/util/time_support.h"

namespace mongo {

    // making a static bool to tell us whether logging of operations is on
    // because this bool depends on replication, and replication is not
    // compiled with coredb. So, in startReplication, we will set this
    // to true
    static bool _logTxnOperations = false;
    static void (*_logTxnToOplog)(GTID gtid, uint64_t timestamp, uint64_t hash, BSONArray& opInfo) = NULL;
    static GTIDManager* txnGTIDManager = NULL;

    TxnCompleteHooks *_completeHooks;

    void setTxnLogOperations(bool val) {
        _logTxnOperations = val;
    }

    bool logTxnOperations() {
        return _logTxnOperations;
    }

    void setLogTxnToOplog(void (*f)(GTID gtid, uint64_t timestamp, uint64_t hash, BSONArray& opInfo)) {
        _logTxnToOplog = f;
    }

    void setTxnGTIDManager(GTIDManager* m) {
        txnGTIDManager = m;
    }

    void setTxnCompleteHooks(TxnCompleteHooks *hooks) {
        _completeHooks = hooks;
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
        uint64_t timestamp = 0;
        uint64_t hash = 0;
        // do this in case we are writing the first entry
        // we put something in that can be distinguished from
        // an initialized GTID that has never been touched
        gtid.inc_primary(); 
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
                    txnGTIDManager->getGTIDForPrimary(&gtid, &timestamp, &hash);
                }
                else {
                    dassert(!txnGTIDManager);
                    timestamp = curTimeMillis64();
                }
                gotGTID = true;
                // In this case, the transaction we are committing has
                // no parent, so we must write the transaction's 
                // logged operations to the opLog, as part of this transaction
                writeOpsToOplog(gtid, timestamp, hash);
            }
        }
        _txn.commit(flags);
        // if the commit of this transaction got a GTID, then notify 
        // the GTIDManager that the commit is now done.
        if (gotGTID && !_initiatingRS) {
            dassert(txnGTIDManager);
            // save the GTID for the client so that
            // getLastError will know what GTID slaves
            // need to be caught up to.
            cc().setLastOp(gtid);
            txnGTIDManager->noteLiveGTIDDone(gtid);
        }

        // These rollback items must be processed after the ydb transaction completes.
        if (hasParent()) {
            _cappedRollback.transfer(_parent->_cappedRollback);
            _nsIndexRollback.transfer(_parent->_nsIndexRollback);
        } else {
            _cappedRollback.commit();
            _nsIndexRollback.commit();
        }
        _retired = true;
    }

    void TxnContext::abort() {
        // The nsindex must rollback before abort.
        _nsIndexRollback.preAbort();
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

    void TxnContext::writeOpsToOplog(GTID gtid, uint64_t timestamp, uint64_t hash) {
        dassert(_logTxnOperations);
        dassert(_logTxnToOplog);
        BSONArray array = _txnOps.arr();
        _logTxnToOplog(gtid, timestamp, hash, array);
    }

    void CappedCollectionRollback::_complete(const bool committed) {
        for (ContextMap::const_iterator it = _map.begin(); it != _map.end(); it++) {
            const string &ns = it->first;
            const Context &c = it->second;
            _completeHooks->noteTxnCompletedInserts(ns, c.insertedPKs, c.nDelta, c.sizeDelta, committed);
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

    /* --------------------------------------------------------------------- */

    void NamespaceIndexRollback::commit() {
        // nothing to do on commit
    }

    void NamespaceIndexRollback::preAbort() {
        _completeHooks->noteTxnAbortedFileOps(_rollback);
    }

    void NamespaceIndexRollback::transfer(NamespaceIndexRollback &parent) {
        TOKULOG(1) << "NamespaceIndexRollback::transfer processing "
                   << parent._rollback.size() << " roll items." << endl;

        // Promote rollback entries to parent.
        set<string> &rollback = parent._rollback;
        rollback.insert(_rollback.begin(), _rollback.end());
    }

    void NamespaceIndexRollback::noteNs(const char *ns) {
        _rollback.insert(ns);
    }

} // namespace mongo
