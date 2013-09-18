/**
*    Copyright (C) 2013 Tokutek Inc.
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

#include "mongo/db/txn_context.h"

#include "mongo/base/counter.h"
#include "mongo/bson/bsonobjiterator.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/gtid.h"
#include "mongo/db/oplog.h"
#include "mongo/db/repl.h"
#include "mongo/db/stats/timer_stats.h"
#include "mongo/db/storage_options.h"
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
    static void (*_logTxnToOplog)(GTID gtid, uint64_t timestamp, uint64_t hash, const deque<BSONObj>& ops) = NULL;
    static void (*_logTxnOpsRef)(GTID gtid, uint64_t timestamp, uint64_t hash, OID& oid) = NULL;
    static void (*_logOpsToOplogRef)(BSONObj o) = NULL;
    static bool (*_shouldLogOpForSharding)(const char *, const char *, const BSONObj &) = NULL;
    static bool (*_shouldLogUpdateOpForSharding)(const char *, const char *, const BSONObj &) = NULL;
    static void (*_startObjForMigrateLog)(BSONObjBuilder &b) = NULL;
    static void (*_writeObjToMigrateLog)(BSONObj &) = NULL;
    static void (*_writeObjToMigrateLogRef)(BSONObj &) = NULL;
    static TimerStats *_oplogInsertStats;
    static Counter64 *_oplogInsertBytesStats;

    static GTIDManager* txnGTIDManager = NULL;

    TxnCompleteHooks *_completeHooks;

    void setLogTxnOpsForReplication(bool val) {
        _logTxnOpsForReplication = val;
    }

    bool logTxnOpsForReplication() {
        return _logTxnOpsForReplication;
    }

    void setLogTxnToOplog(void (*f)(GTID gtid, uint64_t timestamp, uint64_t hash, const deque<BSONObj>& ops)) {
        _logTxnToOplog = f;
    }

    void setLogTxnRefToOplog(void (*f)(GTID gtid, uint64_t timestamp, uint64_t hash, OID& oid)) {
        _logTxnOpsRef = f;
    }

    void setLogOpsToOplogRef(void (*f)(BSONObj o)) {
        _logOpsToOplogRef = f;
    }

    void setOplogInsertStats(TimerStats *oplogInsertStats, Counter64 *oplogInsertBytesStats) {
        _oplogInsertStats = oplogInsertStats;
        _oplogInsertBytesStats = oplogInsertBytesStats;
    }

    void setTxnGTIDManager(GTIDManager* m) {
        txnGTIDManager = m;
    }

    void setTxnCompleteHooks(TxnCompleteHooks *hooks) {
        _completeHooks = hooks;
    }

    void enableLogTxnOpsForSharding(bool (*shouldLogOp)(const char *, const char *, const BSONObj &),
                                    bool (*shouldLogUpdateOp)(const char *, const char *, const BSONObj &),
                                    void (*startObj)(BSONObjBuilder &b),
                                    void (*writeObj)(BSONObj &),
                                    void (*writeObjToRef)(BSONObj &)) {
        _logTxnOpsForSharding = true;
        _shouldLogOpForSharding = shouldLogOp;
        _shouldLogUpdateOpForSharding = shouldLogUpdateOp;
        _startObjForMigrateLog = startObj;
        _writeObjToMigrateLog = writeObj;
        _writeObjToMigrateLogRef = writeObjToRef;
    }

    void disableLogTxnOpsForSharding(void) {
        _logTxnOpsForSharding = false;
        _shouldLogOpForSharding = NULL;
        _shouldLogUpdateOpForSharding = NULL;
        _startObjForMigrateLog = NULL;
        _writeObjToMigrateLog = NULL;
        _writeObjToMigrateLogRef = NULL;
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

    bool shouldLogTxnUpdateOpForSharding(const char *opstr, const char *ns, const BSONObj &oldObj) {
        if (!logTxnOpsForSharding()) {
            return false;
        }
        dassert(_shouldLogUpdateOpForSharding != NULL);
        return _shouldLogUpdateOpForSharding(opstr, ns, oldObj);
    }

    TxnContext::TxnContext(TxnContext *parent, int txnFlags)
            : _txn((parent == NULL) ? NULL : &parent->_txn, txnFlags), 
              _parent(parent),
              _retired(false),
              _txnOps(parent == NULL ? NULL : &parent->_txnOps),
              _txnOpsForSharding(_writeObjToMigrateLogRef,
                                 // transferMods has a maxSize of 1MB, we leave a few hundred bytes for metadata.
                                 1024 * 1024 - 512,
                                 parent == NULL ? NULL : &parent->_txnOpsForSharding),
              _initiatingRS(false)
    {
    }

    TxnContext::~TxnContext() {
        if (!_retired) {
            abort();
        }
    }

    void TxnContext::commitChild(int flags) {
        verify(hasParent());
        // handle work related to logging of transaction for replication
        // this piece must be done before the _txn.commit
        try {
            // This does something
            // a bit dangerous in that it may spill parent's stuff
            // with this child transaction that is committing. If something
            // goes wrong and this child transaction aborts, we will miss
            // some ops
            //
            // This ought to be ok, because we are in this try/catch block
            // where if something goes wrong, we will crash the server.
            // NOTHING better go wrong here, unless under bad rare
            // circumstances
            _txnOps.finishChildCommit();
            // handle work related to logging of transaction for chunk migrations
            if (!_txnOpsForSharding.empty()) {
                transferOpsForShardingToParent();
            }

            _clientCursorRollback.preComplete();
            _txn.commit(flags);
        }
        catch (std::exception &e) {
            StackStringBuilder ssb;
            ssb << "exception during critical section of txn child commit, aborting system: " << e.what();
            rawOut(ssb.str());
            ::abort();
        }

        // These rollback items must be processed after the ydb transaction completes.
        _cappedRollback.transfer(_parent->_cappedRollback);
        _collectionMapRollback.transfer(_parent->_collectionMapRollback);
    }

    void TxnContext::commitRoot(int flags) {
        verify(!hasParent());
        bool gotGTID = false;
        GTID gtid;
        // do this in case we are writing the first entry
        // we put something in that can be distinguished from
        // an initialized GTID that has never been touched
        gtid.inc_primary(); 
        // handle work related to logging of transaction for replication
        // this piece must be done before the _txn.commit
        try {
            if (!_txnOps.empty()) {
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
                writeTxnOpsToMigrateLog();
            }

            _clientCursorRollback.preComplete();
            try {
                _txn.commit(flags);
            }
            catch (std::exception &e) {
                StackStringBuilder ssb;
                ssb << "exception during critical section of txn root commit, aborting system: " << e.what();
                rawOut(ssb.str());
                ::abort();
            }

            // if the commit of this transaction got a GTID, then notify 
            // the GTIDManager that the commit is now done.
            if (gotGTID && !_initiatingRS) {
                // save the GTID for the client so that
                // getLastError will know what GTID slaves
                // need to be caught up to.
                cc().setLastOp(gtid);
                txnGTIDManager->noteLiveGTIDDone(gtid);
            }
        }
        catch (std::exception &e) {
            if (gotGTID && !_initiatingRS) {
                txnGTIDManager->noteLiveGTIDDone(gtid);
            }
        }

        // These rollback items must be processed after the ydb transaction completes.
        _cappedRollback.commit();
        _collectionMapRollback.commit();
    }

    void TxnContext::commit(int flags) {
        verify(!_retired);
        if (hasParent()) {
            commitChild(flags);
        }
        else {
            commitRoot(flags);
        }
        _retired = true;
    }

    void TxnContext::abort() {
        verify(!_retired);
        _clientCursorRollback.preComplete();
        _collectionMapRollback.preAbort();
        _txnOps.abort();
        _txn.abort();
        _cappedRollback.abort();
        _retired = true;
    }

    void TxnContext::logOpForReplication(BSONObj op) {
        dassert(logTxnOpsForReplication());
        _txnOps.appendOp(op);
    }

    void TxnContext::logOpForSharding(BSONObj op) {
        dassert(logTxnOpsForSharding());
        _txnOpsForSharding.append(op);
    }

    bool TxnContext::hasParent() {
        return (_parent != NULL);
    }

    void TxnContext::txnIntiatingRs() {
        _initiatingRS = true;
    }

    void TxnContext::transferOpsForShardingToParent() {
        _txnOpsForSharding.transfer();
    }

    void TxnContext::writeTxnOpsToMigrateLog() {
        dassert(logTxnOpsForSharding());
        dassert(_startObjForMigrateLog != NULL);
        dassert(_writeObjToMigrateLog != NULL);
        BSONObjBuilder b;
        _startObjForMigrateLog(b);
        _txnOpsForSharding.getObjectsOrRef(b);
        BSONObj obj = b.done();
        _writeObjToMigrateLog(obj);
    }

    /* --------------------------------------------------------------------- */

    void CappedCollectionRollback::_complete(const bool committed) {
        for (ContextMap::const_iterator it = _map.begin(); it != _map.end(); it++) {
            const string &ns = it->first;
            const Context &c = it->second;
            _completeHooks->noteTxnCompletedInserts(ns, c.minPK, c.nDelta, c.sizeDelta, committed);
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
            if (parentContext.minPK.isEmpty()) {
                parentContext.minPK = c.minPK;
            } else if (!c.minPK.isEmpty()) {
                dassert(parentContext.minPK <= c.minPK);
            }
            parentContext.nDelta += c.nDelta;
            parentContext.sizeDelta += c.sizeDelta;
        }
    }

    void CappedCollectionRollback::noteInsert(const string &ns, const BSONObj &pk, long long size) {
        Context &c = _map[ns];
        if (c.minPK.isEmpty()) {
            c.minPK = pk.getOwned();
        }
        dassert(c.minPK <= pk);
        c.nDelta++;
        c.sizeDelta += size;
    }

    void CappedCollectionRollback::noteDelete(const string &ns, const BSONObj &pk, long long size) {
        Context &c = _map[ns];
        c.nDelta--;
        c.sizeDelta -= size;
    }

    bool CappedCollectionRollback::hasNotedInsert(const string &ns) {
        const Context &c = _map[ns];
        return !c.minPK.isEmpty();
    }

    /* --------------------------------------------------------------------- */

    void CollectionMapRollback::commit() {
        // nothing to do on commit
    }

    void CollectionMapRollback::preAbort() {
        _completeHooks->noteTxnAbortedFileOps(_namespaces, _dbs);
    }

    void CollectionMapRollback::transfer(CollectionMapRollback &parent) {
        TOKULOG(1) << "CollectionMapRollback::transfer processing "
                   << _namespaces.size() + _dbs.size() << " roll items." << endl;

        // Promote rollback entries to parent.
        parent._namespaces.insert(_namespaces.begin(), _namespaces.end());
        parent._dbs.insert(_dbs.begin(), _dbs.end());
    }

    void CollectionMapRollback::noteNs(const StringData& ns) {
        _namespaces.insert(ns.toString());
    }

    void CollectionMapRollback::noteCreate(const StringData& dbname) {
        _dbs.insert(dbname.toString());
    }

    /* --------------------------------------------------------------------- */

    void ClientCursorRollback::preComplete() {
        _completeHooks->noteTxnCompletedCursors(_cursorIds);
    }

    void ClientCursorRollback::noteClientCursor(long long id) {
        _cursorIds.insert(id);
    }

    TxnOplog::TxnOplog(TxnOplog *parent) : _parent(parent), _spilled(false), _mem_size(0), _mem_limit(storageGlobalParams.txnMemLimit), _refsSize(0) {
        // This is initialized to 1 so that the query in applyRefOp in
        // oplog.cpp can
        _seq = 1;
        if (_parent) { // child inherits the parents seq number and spilled state
            _seq = _parent->_seq + 1;
        }
    }

    TxnOplog::~TxnOplog() {
    }

    void TxnOplog::appendOp(BSONObj o) {
        _seq++;
        _m.push_back(o);
        _mem_size += o.objsize();
        if (_mem_size > _mem_limit) {
            spill();
            _spilled = true;
        }
    }

    bool TxnOplog::empty() const {
        return !_spilled && (_m.size() == 0);
    }

    void TxnOplog::spill() {
        // it is possible to have spill called when there
        // is nothing to actually spill. For instance, when
        // the root commits and we have already spilled,
        // we call spill to write out any remaining ops, of which
        // there may be none
        if (_m.size() > 0) {
            if (!_oid.isSet()) {
                _oid = getOid();
            }
            BSONObjBuilder b;

            // build the _id
            BSONObjBuilder b_id;
            b_id.append("oid", _oid);
            // probably not necessary to increment _seq, but safe to do
            b_id.append("seq", ++_seq);
            b.append("_id", b_id.obj());

            // build the ops array
            BSONArrayBuilder b_a;
            while (_m.size() > 0) {
                BSONObj o = _m.front();
                b_a.append(o);
                _m.pop_front();
                _mem_size -= o.objsize();
            }
            b.append("ops", b_a.arr());

            verify(_m.size() == 0);
            verify(_mem_size == 0);

            // insert it
            dassert(_logOpsToOplogRef);

            BSONObj obj = b.obj();
            TimerHolder timer(&_refsTimer);
            _refsSize += obj.objsize();
            _logOpsToOplogRef(obj);
        }
        else {
            // just a sanity check
            verify(_oid.isSet());
        }
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

    void TxnOplog::writeOpsDirectlyToOplog(GTID gtid, uint64_t timestamp, uint64_t hash) {
        dassert(logTxnOpsForReplication());
        dassert(_logTxnToOplog);
        // log ops
        _logTxnToOplog(gtid, timestamp, hash, _m);
    }

    void TxnOplog::writeTxnRefToOplog(GTID gtid, uint64_t timestamp, uint64_t hash) {
        dassert(logTxnOpsForReplication());
        dassert(_logTxnOpsRef);
        dassert(_oplogInsertStats);
        dassert(_oplogInsertBytesStats);
        _oplogInsertStats->recordMillis(_refsTimer.millis());
        _oplogInsertBytesStats->increment(_refsSize);
        // log ref
        _logTxnOpsRef(gtid, timestamp, hash, _oid);
    }

    void TxnOplog::rootCommit(GTID gtid, uint64_t timestamp, uint64_t hash) {
        if (_spilled) {
            // spill in memory ops if any
            spill();
            // log ref
            writeTxnRefToOplog(gtid, timestamp, hash);
        } else {
            writeOpsDirectlyToOplog(gtid, timestamp, hash);
        }
    }

    void TxnOplog::finishChildCommit() {
        // parent inherits the childs seq number and spilled state
        verify(_seq > _parent->_seq);
        // the first thing we want to do, is if child has spilled,
        // we must get the parent to spill.
        // The parent will have a _seq that is smaller than
        // any seq we used, so the data that is spilled will be
        // correctly positioned behind all of the work we have done
        // in the oplog.refs collection. For that reason, this must be
        // done BEFORE we set the parent's _seq to our _seq + 1
        if (_spilled) {
            _parent->spill();
            _parent->_spilled = _spilled;
        }
        _parent->_seq = _seq+1;
        // move to parent
        for (deque<BSONObj>::iterator it = _m.begin(); it != _m.end(); it++) {
            _parent->appendOp(*it);
        }
    }

    void TxnOplog::abort() {
        // nothing to do on abort
    }

} // namespace mongo
