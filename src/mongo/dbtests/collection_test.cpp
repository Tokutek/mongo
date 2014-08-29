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

#include "mongo/unittest/unittest.h"
#include "mongo/db/collection.h"

#include "mongo/db/client.h"
#include "mongo/db/instance.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespacestring.h"
#include "mongo/util/log.h"

namespace mongo {
namespace unittest {

static const string EMPTY_STRING("");

} // namespace unittest
} // namespace mongo

namespace {

    using boost::shared_ptr;
    using mongo::BSONObj;
    using mongo::BSONObjBuilder;
    using mongo::BSONArrayBuilder;
    using mongo::Client;
    using mongo::DBClientCursor;
    using mongo::DBDirectClient;
    using mongo::StringData;
    using std::auto_ptr;
    using std::endl;
    using std::string;

    class SystemUsersTests : public mongo::unittest::Test {
      protected:
        DBDirectClient _c;
        string _ns;
        StringData _db;
        StringData _coll;
        void setUp() {
            _ns = "collection_tests.system.users";
            _db = mongo::nsToDatabaseSubstring(_ns);
            _coll = mongo::nsToCollectionSubstring(_coll);
            ASSERT(_c.dropDatabase(_db.toString(), NULL));
        }
        void checkIndexes() {
            auto_ptr<DBClientCursor> cur = _c.getIndexes(_ns);

            ASSERT_TRUE(cur->more());
            BSONObj idx = cur->next();
            ASSERT_EQUALS(idx["key"].Obj(), BSON("_id" << 1));

            ASSERT_TRUE(cur->more());
            idx = cur->next();
            ASSERT_EQUALS(idx["key"].Obj(), mongo::extendedSystemUsersKeyPattern);

            ASSERT_FALSE(cur->more());
        }
        void addUser() {
            Client::WriteContext ctx(_ns, mongo::unittest::EMPTY_STRING);
            Client::Transaction txn(DB_SERIALIZABLE);
            _c.insert(_ns, BSON(mongo::GENOID <<
                                "user" << "admin" <<
                                "readOnly" << "false" <<
                                "pwd" << "90f500568434c37b61c8c1ce05fdf3ae" // hash of "password", it turns out
                                ));
            txn.commit();
        }
        void printAllUsers() {
            Client::AlternateTransactionStack altStack;
            Client::ReadContext ctx(_ns, mongo::unittest::EMPTY_STRING);
            Client::Transaction txn(DB_SERIALIZABLE);
            auto_ptr<DBClientCursor> cur = _c.query(_ns, BSONObj());
            mongo::unittest::log() << "users:" << endl;
            while (cur->more()) {
                mongo::unittest::log() << "\t" << cur->next() << endl;
            }
        }
    };

    TEST_F(SystemUsersTests, DefaultIndexCreate) {
        Client::WriteContext ctx(_ns, mongo::unittest::EMPTY_STRING);
        Client::Transaction txn(DB_SERIALIZABLE);
        _c.createCollection(_ns);
        txn.commit();
        checkIndexes();
    }

    TEST_F(SystemUsersTests, DefaultIndexInsert) {
        addUser();
        checkIndexes();
    }

    TEST_F(SystemUsersTests, CanAuth) {
        addUser();
        _c.simpleCommand("admin", NULL, "closeAllDatabases");
        string err;
        bool ok = _c.auth(_db.toString(), "admin", "password", err);
        ASSERT_EQUALS(err, "");
        ASSERT_TRUE(ok);
    }

    TEST_F(SystemUsersTests, RectifyBug673) {
        addUser();
        _c.simpleCommand("admin", NULL, "closeAllDatabases");
        BSONArrayBuilder b;
        {
            BSONObjBuilder idBuilder(b.subobjStart());
            idBuilder.append("key", BSON("_id" << 1));
            idBuilder.appendBool("unique", true);
            idBuilder.append("ns", _ns);
            idBuilder.append("name", "_id_");
            idBuilder.appendBool("clustering", true);
            idBuilder.done();
        }
        {
            BSONObjBuilder userBuilder(b.subobjStart());
            userBuilder.append("key", mongo::oldSystemUsersKeyPattern);
            userBuilder.appendBool("unique", true);
            userBuilder.append("ns", _ns);
            userBuilder.append("name", "user_1");
            userBuilder.done();
        }
        {
            BSONObjBuilder userBuilder(b.subobjStart());
            userBuilder.append("key", mongo::extendedSystemUsersKeyPattern);
            userBuilder.appendBool("unique", true);
            userBuilder.append("ns", _ns);
            userBuilder.append("name", mongo::extendedSystemUsersIndexName);
            userBuilder.done();
        }
        shared_ptr<mongo::Collection> d;
        {
            Client::UpgradingSystemUsersScope usus;
            Client::WriteContext ctx(_ns, mongo::unittest::EMPTY_STRING);
            Client::Transaction txn(DB_SERIALIZABLE);
            // This would throw if we didn't handle the corruption fix properly
            d = mongo::Collection::make(mongo::Collection::serialize(_ns, BSONObj(), BSON("_id" << 1), 0ULL, b.arr()));
            ASSERT(d);
            ASSERT_GREATER_THAN(d->findIndexByKeyPattern(mongo::extendedSystemUsersKeyPattern), 0);
            ASSERT_LESS_THAN(d->findIndexByKeyPattern(mongo::oldSystemUsersKeyPattern), 0);
            txn.commit();
        }
        printAllUsers();
        _c.simpleCommand("admin", NULL, "closeAllDatabases");
        printAllUsers();
        _c.simpleCommand("admin", NULL, "closeAllDatabases");
        string err;
        bool ok = _c.auth(_db.toString(), "admin", "password", err);
        ASSERT_EQUALS(err, "");
        ASSERT_TRUE(ok);
    }

}
