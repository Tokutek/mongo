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

#ifndef MONGO_DB_TXNCONTEXT_H
#define MONGO_DB_TXNCONTEXT_H

#include "mongo/pch.h"

#include <db.h>

#include "mongo/db/jsobj.h"
#include "mongo/db/storage/txn.h"

namespace mongo {

    class GTID;
    class GTIDManager;

    void setLogTxnOpsForReplication(bool val);
    bool logTxnOpsForReplication();
    void enableLogTxnOpsForSharding(bool (*shouldLogOp)(const char *, const char *, const BSONObj &),
                                    bool (*shouldLogUpdateOp)(const char *, const char *, const BSONObj &, const BSONObj &),
                                    void (*writeOps)(const vector<BSONObj> &));
    void disableLogTxnOpsForSharding(void);
    bool shouldLogTxnOpForSharding(const char *opstr, const char *ns, const BSONObj &obj);
    bool shouldLogTxnUpdateOpForSharding(const char *opstr, const char *ns, const BSONObj &oldObj, const BSONObj &newObj);
    void setLogTxnToOplog(void (*f)(GTID gtid, uint64_t timestamp, uint64_t hash, BSONArray& opInfo));
    void setTxnGTIDManager(GTIDManager* m);

    class TxnCompleteHooks {
    public:
        virtual ~TxnCompleteHooks() { }
        virtual void noteTxnCompletedInserts(const string &ns, const BSONObj &minPK,
                                             long long nDelta, long long sizeDelta,
                                             bool committed) {
            assertNotImplemented();
        }
        virtual void noteTxnAbortedFileOps(const set<string> &namespaces) {
            assertNotImplemented();
        }
    private:
        void assertNotImplemented() {
            msgasserted(16778, "bug: TxnCompleteHooks not set");
        }
    };

    void setTxnCompleteHooks(TxnCompleteHooks *hooks);

    // Class to handle rollback of in-memory stats for capped collections.
    class CappedCollectionRollback {
    public:
        void commit();

        void abort();

        void transfer(CappedCollectionRollback &parent);

        void noteInsert(const string &ns, const BSONObj &pk, long long size);

        void noteDelete(const string &ns, const BSONObj &pk, long long size);

        bool hasNotedInsert(const string &ns);

    private:
        void _complete(const bool committed);

        struct Context {
            Context() : nDelta(0), sizeDelta(0) { }
            BSONObj minPK;
            long long nDelta;
            long long sizeDelta;
        };
        typedef map<string, Context> ContextMap;
        ContextMap _map;
    };

    // Class to handle rollback of in-memory modifications to the namespace index
    // On abort, we simply reload the map entry for each ns touched, bringing it in
    // sync with whatever is on disk in the nsdb.
    class NamespaceIndexRollback {
    public:
        void commit();

        // Must be called before the actual transaction aborts.
        void preAbort();

        void transfer(NamespaceIndexRollback &parent);

        void noteNs(const char *ns);

    private:
        set<string> _rollback;
    };

    // class to wrap operations surrounding a storage::Txn.
    // as of now, includes writing of operations to opLog
    // and the committing/aborting of storage::Txn
    class TxnContext: boost::noncopyable {
        storage::Txn _txn;
        TxnContext* _parent;
        bool _retired;
        //
        // a BSON Array that will hold all of the operations done by
        // this transaction. If the array gets too large, its contents
        // will spill into the localOpRef collection on commit,
        //
        BSONArrayBuilder _txnOps;
        uint64_t _numOperations; //number of operations added to _txnOps

        vector<BSONObj> _txnOpsForSharding;

        // this is a hack. During rs initiation, where we log a comment
        // to the opLog, we don't have a gtidManager available yet. We
        // set this bool to let commit know that it is ok to not have a gtidManager
        // and to just write to the opLog with a GTID of (0,0)
        bool _initiatingRS;

        CappedCollectionRollback _cappedRollback;
        NamespaceIndexRollback _nsIndexRollback;

    public:
        TxnContext(TxnContext *parent, int txnFlags);
        ~TxnContext();
        void commit(int flags);
        void abort();
        /** @return the managed DB_TXN object */
        DB_TXN *db_txn() const { return _txn.db_txn(); }
        /** @return true iff this transaction is live */
        bool isLive() const { return _txn.isLive(); }
        /** @return true iff this transaction is read only */
        bool readOnly() const { return (_txn.flags() & DB_TXN_READ_ONLY) != 0; }
        /** @return true iff this transaction has serializable isolation.
         *          note that a child transaction always inherits isolation. */
        bool serializable() const {
            return (_txn.flags() & DB_SERIALIZABLE) != 0 ||
                   (_parent != NULL && _parent->serializable());
        }
        // log an operations, represented in op, to _txnOps
        // if and when the root transaction commits, the operation
        // will be added to the opLog
        void logOpForReplication(BSONObj op);        
        void logOpForSharding(BSONObj op);        
        bool hasParent();
        void txnIntiatingRs();

        CappedCollectionRollback &cappedRollback() {
            return _cappedRollback;
        }

        NamespaceIndexRollback &nsIndexRollback() {
            return _nsIndexRollback;
        }

    private:
        // transfer operations in _txnOps to _parent->_txnOps
        void transferOpsToParent();
        void writeOpsToOplog(GTID gtid, uint64_t timestamp, uint64_t hash);
        void transferOpsForShardingToParent();
        void writeTxnOpsToMigrateLog();
    };

} // namespace mongo

#endif // MONGO_DB_STORAGE_TXN_H
