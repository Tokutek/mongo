/** @file remote_transaction.h */

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

#pragma once

#include "mongo/pch.h"

namespace mongo {

    class DBClientWithCommands;
    class BSONObj;

    /**
       RemoteTransaction manages the lifetime of a multi-statement transaction on another machine.
       When created, it begins a transaction on the remote machine, and when destroyed, if the transaction is still live, it performs a rollback.

       You should only issue requests related to this transaction along this connection.
       You should consider getting the connection from a ScopedDbConnection if possible.
       You should not close the connection until the transaction has committed or rolled back.

       Example:

           scoped_ptr<ScopedDbConnection> conn(ScopedDbConnection::getScopedDbConnection("localhost:27017"));
           {
               // If the insert to ns2.mycoll fails below, the entire transaction is rolled back.
               RemoteTransaction txn(conn->conn());
               conn->get()->insert("ns1.mycoll", BSON("a" << 1 << "b" << 1));
               conn->get()->insert("ns2.mycoll", BSON("x" << 2 << "y" << 2));
               txn.commit();
           }
           conn->done();
     */
    class RemoteTransaction : boost::noncopyable {
        DBClientWithCommands *_conn;
      public:
        /** Creates a remote transaction using a connection.
            @param conn -- The connection to use for this transaction.
            @param isolation -- What isolation level to use.  Possible values are serializable, mvcc (default), and readUncommitted.
        */
        RemoteTransaction(DBClientWithCommands &conn, const string &isolation = "mvcc");
        /** Rolls back the transaction if necessary. */
        ~RemoteTransaction();
        /** Commits the transaction.
            @param res -- pointer to object to return the result of the commit
            @return true -- iff the commit was successful
         */
        bool commit(BSONObj *res = NULL);
        /** Rolls back the transaction.
            @param res -- pointer to object to return the result of the rollback
            @return true -- iff the rollback was successful
         */
        bool rollback(BSONObj *res = NULL);
    };

} // namespace mongo
