/** @file remote_loader.h */

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

#include "mongo/client/remote_transaction.h"
#include "mongo/db/jsobj.h"

namespace mongo {

    class DBClientWithCommands;

    /**
       RemoteLoader manages the lifetime of a bulk load operation on another machine.
       When created, it begins a transaction on the remote machine and starts a bulk load operation, and when destroyed, if the transaction is still live, it performs a rollback of the load operation and the transaction.

       You should only issue requests related to this transaction along this connection.
       You should consider getting the connection from a ScopedDbConnection if possible.
       You should not close the connection until the load has committed or aborted.
       You should not attempt to access the collection undergoing a load from another connection.

       Example:

           scoped_ptr<ScopedDbConnection> conn(ScopedDbConnection::getScopedDbConnection("localhost:27017"));
           {
               // If the insert to ns2.mycoll fails below, the entire transaction is rolled back.
               RemoteLoader loader(conn->conn(), "mydb", BSON("ns" << "coll" << "indexes" << BSON_ARRAY()));
               conn->get()->insert("mydb.coll", BSON("a" << 1 << "b" << 1));
               conn->get()->insert("mydb.coll", BSON("x" << 2 << "y" << 2));
               loader.commit();
           }
           conn->done();
     */
    class RemoteLoader : boost::noncopyable {
        DBClientWithCommands *_conn;
        string _db;
        RemoteTransaction _rtxn;
        bool _usingLoader;
        BSONObj _commandObj;
        static void beginLoadCmd(const string &ns, const vector<BSONObj> &indexes, const BSONObj &options,
                                 BSONObjBuilder &b);
        void begin(const BSONObj &obj);
      public:
        /** Creates a bulk loader using a connection.
            @param conn -- The connection to use for this transaction.
            @param db -- The db in which to load a collection.
            @param obj -- The beginLoad command object to use.
        */
        RemoteLoader(DBClientWithCommands &conn, const string &db, const BSONObj &obj);
        /** Creates a bulk loader using a connection.  Generates the beginLoad command from the parameters.
            @param conn -- The connection to use for this transaction.
            @param db -- The db in which to load a collection.
            @param ns -- The name of the collection to load.
            @param indexes -- A list of indexes to create, given as index spec objects.
            @param options -- Additional createCollection options.
         */
        RemoteLoader(DBClientWithCommands &conn, const string &db,
                     const string &ns, const vector<BSONObj> &indexes, const BSONObj &options);
        /** Aborts the load if necessary. */
        ~RemoteLoader();
        /** Commits the load.
            @param res -- pointer to object to return the result of the commit
            @return true -- iff the commit was successful
         */
        bool commit(BSONObj *res = NULL);
        /** Aborts the load.
            @param res -- pointer to object to return the result of the abort
            @return true -- iff the abort was successful
         */
        bool abort(BSONObj *res = NULL);
    };

} // namespace mongo
