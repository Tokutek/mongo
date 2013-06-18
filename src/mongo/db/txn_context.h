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
                                    void (*startObj)(BSONObjBuilder &),
                                    void (*writeObj)(BSONObj &),
                                    void (*writeObjToRef)(BSONObj &));
    void disableLogTxnOpsForSharding(void);
    bool shouldLogTxnOpForSharding(const char *opstr, const char *ns, const BSONObj &obj);
    bool shouldLogTxnUpdateOpForSharding(const char *opstr, const char *ns, const BSONObj &oldObj, const BSONObj &newObj);
    void setLogTxnToOplog(void (*)(GTID gtid, uint64_t timestamp, uint64_t hash, BSONArray& opInfo));
    void setLogTxnRefToOplog(void (*f)(GTID gtid, uint64_t timestamp, uint64_t hash, OID& oid));
    void setLogOpsToOplogRef(void (*f)(BSONObj o));
    void setTxnGTIDManager(GTIDManager* m);

    class TxnCompleteHooks {
    public:
        virtual ~TxnCompleteHooks() { }
        virtual void noteTxnCompletedInserts(const string &ns, const BSONObj &minPK,
                                             long long nDelta, long long sizeDelta,
                                             bool committed) {
            assertNotImplemented();
        }
        virtual void noteTxnAbortedFileOps(const set<string> &namespaces, const set<string> &dbs) {
            assertNotImplemented();
        }
        virtual void noteTxnCompletedCursors(const set<long long> &cursorIds) {
            assertNotImplemented();
        }
    private:
        void assertNotImplemented() {
            msgasserted(16778, "bug: TxnCompleteHooks not set");
        }
    };

    void setTxnCompleteHooks(TxnCompleteHooks *hooks);

    // Class to handle rollback of in-memory stats for capped collections.
    class CappedCollectionRollback : boost::noncopyable {
    public:
        // Called after txn commit.
        void commit();

        // Called after txn abort.
        void abort();

        // Called after commit or abort if a parent exists.
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
    class NamespaceIndexRollback : boost::noncopyable {
    public:
        // Called after txn commit.
        void commit();

        // Called before txn abort.
        void preAbort();

        void transfer(NamespaceIndexRollback &parent);

        void noteNs(const StringData& ns);

        void noteCreate(const StringData& dbname);

    private:
        set<string> _dbs;
        set<string> _namespaces;
    };

    // Handles killing cursors for multi-statement transactions, before they
    // commit or abort.
    class ClientCursorRollback : boost::noncopyable {
    public:
        // Called before txn commit or abort, even if a parent exists (ie: no transfer)
        void preComplete();

        void noteClientCursor(long long id);

    private:
        void _complete();
        set<long long> _cursorIds;
    };

    /**
       SpillableVector holds a vector of BSONObjs, and if that vector gets too big (>= maxSize), it
       starts spilling those objects to a backing collection, to be referenced later.

       SpillableVector labels each of the spilled entries with an OID and a sequence number in the
       _id field.  Each entry in the collection is of the form (where n is from a sequence of
       integers starting with 0 for each new OID):

           {
             _id: {"oid": ObjectID("..."),
                   "seq": n},
               a: [
                    ..., // user objects are here
                  ]
           }

       SpillableVector supports getObjectsOrRef(), which appends to a given BSONObjBuilder either a
       reference to the OID used to spill objects, or a BSONArray containing the spilled objects.

       SpillableVector also supports transfer(), which appends its objects to a parent
       SpillableVector.
    */
    class SpillableVector : boost::noncopyable {
        void (*_writeObjToRef)(BSONObj &);
        vector<BSONObj> _vec;
        size_t _curSize;
        const size_t _maxSize;
        SpillableVector *_parent;
        OID _oid;

        bool _curObjInited;
        BufBuilder _buf;
        long long _seq;
        long long *_curSeqNo;
        scoped_ptr<BSONObjBuilder> _curObjBuilder;
        scoped_ptr<BSONArrayBuilder> _curArrayBuilder;
      public:
        SpillableVector(void (*writeObjToRef)(BSONObj &), size_t maxSize, SpillableVector *parent);

        /** @return true iff there have been no objects appended yet. */
        bool empty() const {
            bool isEmpty = _curSize == 0;
            if (isEmpty) {
                dassert(_vec.empty());
            }
            return isEmpty;
        }

        void append(const BSONObj &o);
        void getObjectsOrRef(BSONObjBuilder &b);
        void transfer();

      private:
        bool spilling() const {
            return _curSize >= _maxSize;
        }
        void initCurObj();
        void finish();
        void spillCurObj();
        void spillOneObject(BSONObj obj);
        void spillAllObjects();
    };

    // Each TxnOplog gathers a transaction's operations that need to be put in the oplog
    // when the root transaction commits.  These operations should be maintained in memory
    // unless the size of the list becomes big.  When this happens, the operations are 
    // spilled into the oplog.refs collection indexed by an OID assigned to the root 
    // transaction and a sequence number.  This allows a query by OID to find the operations
    // in insertion order.
    //
    // The documents spilled to the oplog.refs contain an array of operations. The mem_limit
    // parameter limits the size of this array.  We want to pack as many documents into the
    // array and not exceed the limit.
    //
    // TODO The current algorithm is defeated by child transactions that only add one operation.
    // Since spills happen when child transactions commit, all of the child transactions operations
    // will be spilled.  If there is only one, then the size of the entries in the oplog.refs 
    // collection will be small.  Need a new algorithm to fix this problem.
    class TxnOplog : boost::noncopyable {
    public:
        TxnOplog(TxnOplog *parent);
        ~TxnOplog();
        
        // Append an op to the txn's oplog list
        void appendOp(BSONObj o);
        
        // Returns true if the TxnOplog does not contain any ops
        bool empty() const;

        // Commit a root txn
        void rootCommit(GTID gtid, uint64_t timestamp, uint64_t hash);

        // Commit a child txn
        void finishChildCommit();

        // Abort a txn
        void abort();

    private:
        // Spill memory ops to a document in a collection
        void spill();

        // Get the OID assigned to this txn.  Assign one if not already assigned.
        OID getOid();

        // writes operations directly to the oplog with a BSONArray
        void writeOpsDirectlyToOplog(GTID gtid, uint64_t timestamp, uint64_t hash);

        // writes a reference to the operations that exist in oplog.refs to the oplog
        void writeTxnRefToOplog(GTID gtid, uint64_t timestamp, uint64_t hash);

    private:
        TxnOplog *_parent;
        // bool that states if THIS TxnOplog has spilled. It does
        // not reflect the state of the parent.
        bool _spilled;
        size_t _mem_size, _mem_limit;
        deque<BSONObj> _m;
        OID _oid;
        long long _seq;
    };

    // class to wrap operations surrounding a storage::Txn.
    // as of now, includes writing of operations to opLog
    // and the committing/aborting of storage::Txn
    class TxnContext: boost::noncopyable {
        storage::Txn _txn;
        TxnContext* _parent;
        bool _retired;

        TxnOplog _txnOps;

        SpillableVector _txnOpsForSharding;

        // this is a hack. During rs initiation, where we log a comment
        // to the opLog, we don't have a gtidManager available yet. We
        // set this bool to let commit know that it is ok to not have a gtidManager
        // and to just write to the opLog with a GTID of (0,0)
        bool _initiatingRS;

        CappedCollectionRollback _cappedRollback;
        NamespaceIndexRollback _nsIndexRollback;
        ClientCursorRollback _clientCursorRollback;

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
            return (_txn.flags() & DB_SERIALIZABLE) != 0;
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

        ClientCursorRollback &clientCursorRollback() {
            return _clientCursorRollback;
        }

    private:
        // transfer operations in _txnOps to _parent->_txnOps
        void transferOpsToParent();
        void transferOpsForShardingToParent();
        void writeTxnOpsToMigrateLog();
    };

} // namespace mongo

#endif // MONGO_DB_STORAGE_TXN_H
