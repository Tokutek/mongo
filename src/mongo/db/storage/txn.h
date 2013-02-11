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

#ifndef MONGO_DB_STORAGE_TXN_H
#define MONGO_DB_STORAGE_TXN_H

#include "mongo/pch.h"

#include <db.h>

namespace mongo {

    namespace storage {

        /**
         * RAII wrapper for a DB_TXN.  Closely tied to the transaction stored in cc().
         *
         * When created, it creates a child of the current client's transaction (cc().transaction()).
         * When committed or aborted, it resets the current client's transaction to this transaction's parent.
         * When destroyed, it automatically aborts.
         *
         * Example:
         *  {
         *      Transaction t1; // creates a transaction
         *      {
         *          Transaction t2; // creates a child of t1
         *          t2.commit();
         *      } // nothing happens
         *      {
         *          Transaction t3; // creates a child of t1
         *          {
         *              Transaction t4; // creates a child of t4
         *          } // aborts t4
         *          t3.abort();
         *      } // nothing happens
         *      t1.commit();
         *  }
         */
        class Transaction : boost::noncopyable {
            public:
                Transaction();
                ~Transaction();
                void commit();
                void abort();
                inline DB_TXN *txn() const {
                    return _txn;
                }
                inline bool is_root() const {
                    return _parent == NULL;
                }

            private:
                DB_TXN *_txn;
                Transaction *_parent;
                bool _retired;
        };

    } // namespace storage

} // namespace mongo

#endif // MONGO_DB_STORAGE_TXN_H
