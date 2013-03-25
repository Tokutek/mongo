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

#ifndef MONGO_DB_TXNCONTEXT_H
#define MONGO_DB_TXNCONTEXT_H

#include "mongo/pch.h"
#include "mongo/db/storage/txn.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/gtid.h"

#include <db.h>

namespace mongo {

    void setTxnLogOperations(bool val);
    void setLogTxnToOplog(void (*f)(BSONObj id, BSONArray& opInfo));
    void setTxnGTIDManager(GTIDManager* m);

    // class to wrap operations surrounding a storage::Txn.
    // as of now, includes writing of operations to opLog
    // and the committing/aborting of storage::Txn
    class TxnContext: boost::noncopyable {
        storage::Txn _txn;
        TxnContext* _parent;
        //
        // a BSON Array that will hold all of the operations done by
        // this transaction. If the array gets too large, its contents
        // will spill into the localOpRef collection on commit,
        //
        BSONArrayBuilder _txnOps;
        uint64_t _numOperations; //number of operations added to _txnOps

        public:
        TxnContext(TxnContext *parent, int txnFlags);
        ~TxnContext();
        void commit(int flags);
        void abort();
        /** @return the managed DB_TXN object */
        DB_TXN *db_txn() const { return _txn.db_txn(); }
        /** @return true iff this transaction is live */
        bool isLive() const { return _txn.isLive(); }
        /** @return true iff this is a read only transaction */
        bool isReadOnly() const { return _txn.isReadOnly(); };
        // log an operations, represented in op, to _txnOps
        // if and when the root transaction commits, the operation
        // will be added to the opLog
        void logOp(BSONObj op);        
        bool hasParent();

        private:
        // transfer operations in _txnOps to _parent->_txnOps
        void transferOpsToParent();
        void writeOpsToOplog(GTID gtid);
    };


} // namespace mongo

#endif // MONGO_DB_STORAGE_TXN_H
