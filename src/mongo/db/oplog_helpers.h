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

namespace mongo {

    // class of helpers for opLog stuff
    class OpLogHelpers{

        public:
        static logComment(BSONObj comment, TxnContext* txn);
        static logInsert(const char* ns, BSONObj row, TxnContext* txn);
        static logUpdate(const char* ns, BSONObj oldRow, BSONObj newRow, TxnContext* txn);
        static logDelete(const char* ns, BSONObj row, TxnContext* txn);
    };


} // namespace mongo

#endif // MONGO_DB_STORAGE_TXN_H
