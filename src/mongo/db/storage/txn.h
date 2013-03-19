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
         * Wraps a DB_TXN in an exception-safe object.
         * The destructor calls abort() unless it's already committed.
         * Knows whether it's read-only (useful for the cursor).
         */
        class Txn : boost::noncopyable {
            const int _flags;
            DB_TXN *_db_txn;
            void retire();
          public:
            Txn(const Txn *parent, int flags);
            ~Txn();
            void commit(int flags);
            void abort();

            /** @return the managed DB_TXN object */
            DB_TXN *db_txn() const { return _db_txn; }
            /** @return true iff this transaction is live */
            bool isLive() const { return _db_txn != NULL; }
            /** @return true iff this is a read only transaction */
            bool isReadOnly() const;
        };

    } // namespace storage

} // namespace mongo

#endif // MONGO_DB_STORAGE_TXN_H
