// SERVER-8625: Test that dbAdmins can view index definitions.
var conn = MongoRunner.runMongod({auth : ""});

var adminDB = conn.getDB("admin");
var testDB = conn.getDB("testdb");

testDB.foo.insert({a:1});

testDB.addUser({user:'dbAdmin',
                 pwd:'password',
                 roles:['dbAdmin']});

adminDB.addUser({user:'admin',
                 pwd:'password',
                 roles:['userAdminAnyDatabase']}); // To disable localhost auth bypass

testDB.auth('dbAdmin', 'password');
testDB.foo.ensureIndex({a:1});
assert.eq(5, testDB.system.indexes.count()); // 2 for system.users, 2 for foo, 1 for system.namespaces.$_id_
var indexDoc = testDB.system.indexes.findOne({key:{a:1}});
printjson(indexDoc);
assert.neq(null, indexDoc);
assert.eq(5, testDB.system.indexes.stats().count);
