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
#include "mongo/db/gtid.h"
#include "mongo/db/oplog.h"
#include "mongo/db/repl.h"
#include "mongo/db/txn_context.h"
#include "mongo/db/storage/env.h"

#include "mongo/s/d_logic.h"

#include "mongo/util/time_support.h"

namespace mongo {

    // making a static bool to tell us whether logging of operations is on
    // because this bool depends on replication, and replication is not
    // compiled with coredb. So, in startReplication, we will set this
    // to true
    static bool _logTxnOpsForReplication = false;
    static bool _logTxnOpsForSharding = false;
    static void (*_logTxnToOplog)(GTID gtid, uint64_t timestamp, uint64_t hash, BSONArray& opInfo) = NULL;
    static bool (*_shouldLogOpForSharding)(const char *, const char *, const BSONObj &) = NULL;
    static bool (*_shouldLogUpdateOpForSharding)(const char *, const char *, const BSONObj &, const BSONObj &) = NULL;
    static void (*_writeOpsToMigrateLog)(const vector<BSONObj> &) = NULL;

    static GTIDManager* txnGTIDManager = NULL;

    TxnCompleteHooks *_completeHooks;

    void setLogTxnOpsForReplication(bool val) {
        _logTxnOpsForReplication = val;
    }

    bool logTxnOpsForReplication() {
        return _logTxnOpsForReplication;
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

    void enableLogTxnOpsForSharding(bool (*shouldLogOp)(const char *, const char *, const BSONObj &),
                                    bool (*shouldLogUpdateOp)(const char *, const char *, const BSONObj &, const BSONObj &),
                                    void (*writeOps)(const vector<BSONObj> &)) {
        _logTxnOpsForSharding = true;
        _shouldLogOpForSharding = shouldLogOp;
        _shouldLogUpdateOpForSharding = shouldLogUpdateOp;
        _writeOpsToMigrateLog = writeOps;
    }

    void disableLogTxnOpsForSharding(void) {
        _logTxnOpsForSharding = false;
        _shouldLogOpForSharding = NULL;
        _shouldLogUpdateOpForSharding = NULL;
        _writeOpsToMigrateLog = NULL;
    }

    bool logTxnOpsForSharding() {
        return _logTxnOpsForSharding;
    }

    bool shouldLogTxnOpForSharding(const char *opstr, const char *ns, const BSONObj &row) {
        if (!logTxnOpsForSharding()) {
            return false;
        }
        dassert(_shouldLogOpForSharding != NULL);
        return _shouldLogOpForSharding(opstr, ns, row);
    }

    bool shouldLogTxnUpdateOpForSharding(const char *opstr, const char *ns, const BSONObj &oldObj, const BSONObj &newObj) {
        if (!logTxnOpsForSharding()) {
            return false;
        }
        dassert(_shouldLogUpdateOpForSharding != NULL);
        return _shouldLogUpdateOpForSharding(opstr, ns, oldObj, newObj);
    }

    TxnContext::TxnContext(TxnContext *parent, int txnFlags)
            : _txn((parent == NULL) ? NULL : &parent->_txn, txnFlags), 
              _parent(parent),
              _retired(false),
              _txnOps(parent == NULL ? NULL : &parent->_txnOps),
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
        // do this in case we are writing the first entry
        // we put something in that can be distinguished from
        // an initialized GTID that has never been touched
        gtid.inc_primary(); 
        // handle work related to logging of transaction for replication
        if (hasParent()) {
            // In this case, what transaction we are committing has a parent
            // and therefore we must transfer the opLog information from 
            // this transaction to the parent
            _txnOps.childCommit();
        } else if (!_txnOps.empty()) {
            uint64_t timestamp = 0;
            uint64_t hash = 0;
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
            dassert(logTxnOpsForReplication());
            dassert(_logTxnToOplog);
            _txnOps.rootCommit(gtid, timestamp, hash);
        }
        // handle work related to logging of transaction for chunk migrations
        if (!_txnOpsForSharding.empty()) {
            if (hasParent()) {
                transferOpsForShardingToParent();
            }
            else {
                writeTxnOpsToMigrateLog();
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
        _txnOps.abort();
        _txn.abort();
        _cappedRollback.abort();
        _retired = true;
    }

    void TxnContext::logOpForReplication(BSONObj op) {
        dassert(logTxnOpsForReplication());
        _txnOps.append(op, true);
    }

    void TxnContext::logOpForSharding(BSONObj op) {
        dassert(logTxnOpsForSharding());
        _txnOpsForSharding.push_back(op);
    }

    bool TxnContext::hasParent() {
        return (_parent != NULL);
    }

    void TxnContext::txnIntiatingRs() {
        _initiatingRS = true;
    }

    void TxnContext::transferOpsForShardingToParent() {
        _parent->_txnOpsForSharding.insert(_parent->_txnOpsForSharding.end(),
                                           _txnOpsForSharding.begin(), _txnOpsForSharding.end());
    }

    void TxnContext::writeTxnOpsToMigrateLog() {
        dassert(logTxnOpsForSharding());
        dassert(_writeOpsToMigrateLog != NULL);
        _writeOpsToMigrateLog(_txnOpsForSharding);
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

    TxnOplog::TxnOplog(TxnOplog *parent) : _parent(parent), _spilled(false), _mem_size(0), _mem_limit(1) {
        _seq = 0;
        if (_parent) { // child inherits the parents seq number and spilled state
            _seq = _parent->_seq;
            _spilled = _parent->_spilled;
        }
    }

    TxnOplog::~TxnOplog() {
    }

    void TxnOplog::append(BSONObj o, bool maybe_spill) {
        _seq++;
        _m.push_back(o);
        _mem_size += o.objsize();
        if (_mem_limit > 0 && _m.size() > 1 && _mem_size > _mem_limit) {
            if (!_oid.isSet()) {
                _oid = getOid();
            }
            if (maybe_spill) {
                spill();
            }
            _spilled = true; // aways set the spilled state
        }
    }

    bool TxnOplog::empty() const {
        return _seq == 0;
    }

    void TxnOplog::spill() {
        long long start_seq = _seq - _m.size();
        for (vector<BSONObj>::iterator it = _m.begin(); it != _m.end(); it++) {
            BSONObjBuilder b;
            // build the _id
            BSONObjBuilder b_id;
            b_id.append("oid", _oid);
            b_id.append("seq", start_seq++);
            b.append("_id", b_id.obj());
            // build the val
            BSONArrayBuilder b_v;
            b_v.append(*it);
            b.append("ops", b_v.arr());
            // insert it
            BSONObj o = b.obj();
            insertOplogRefs(o);
        }
        _mem_size = 0;
        _m.clear();
    }

    OID TxnOplog::getOid() {
        if (!_oid.isSet()) {
            if (!_parent) {
                _oid = OID::gen();
            } else {
                _oid = _parent->getOid();
            }
        }
        return _oid;
    }

    void TxnOplog::rootCommit(GTID gtid, uint64_t timestamp, uint64_t hash) {
        if (_spilled) {
            // spill in memory ops if any
            spill();
            // log ref
            logTransactionOpsRef(gtid, timestamp, hash, _oid);
        } else {
            // build array of in memory ops
            BSONArrayBuilder b;
            for (vector<BSONObj>::iterator it = _m.begin(); it != _m.end(); it++) {
                b.append(*it);
            }
            BSONArray a = b.arr();
            // log ops
            logTransactionOps(gtid, timestamp, hash, a);
        }
    }

    void TxnOplog::childCommit() {
        // parent inherits the childs seq number and spilled state
        verify(_seq >= _parent->_seq);
        _parent->_seq = _seq;
        if (_spilled)
            _parent->_spilled = _spilled;
        if (_spilled) {
            spill(); // spill remainder
        } else {
            // move to parent
            for (vector<BSONObj>::iterator it = _m.begin(); it != _m.end(); it++) {
                _parent->append(*it, false);
            }
        }
    }

    void TxnOplog::abort() {
        // nothing to do on abort
    }

} // namespace mongo
