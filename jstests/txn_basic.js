// verify that we can create multiple indexes with the same key

// try committing a transaction without having begun one
t = db.runCommand({"commitTransaction":1});
assert(t.ok == 1);
assert(t.status == "no transaction exists, no-op");

t = db.runCommand({"rollbackTransaction":1});
assert(t.ok == 1);
assert(t.status == "no transaction exists, no-op");

// begin a transaction
t = db.runCommand({"beginTransaction":1});
assert(t.ok == 1);
assert(t.status == "transaction began");

// trying to begin again should work, but with different status
t = db.runCommand({"beginTransaction":1});
assert(t.ok == 1);
assert(t.status == "transaction exists, no-op");

// try committing a transaction
t = db.runCommand({"commitTransaction":1});
assert(t.ok == 1);
assert(t.status == "transaction committed");

// begin a transaction
t = db.runCommand({"beginTransaction":1});
assert(t.ok == 1);
assert(t.status == "transaction began");

// try rolling back a transaction
t = db.runCommand({"rollbackTransaction":1})
assert(t.ok == 1);
assert(t.status == "transaction rolled back");

// make sure that we cannot create a collection
// via an insert in a multi statement transaction
db.jstests_txn_basic.drop(); 
t = db.runCommand({"beginTransaction":1});
assert(t.ok == 1);
assert(t.status == "transaction began");
db.jstests_txn_basic.insert({a:1});
t = db.runCommand({"getLastError":1});
assert(t.code == 16468);
t = db.runCommand({"commitTransaction":1});
assert(t.ok == 1);
assert(t.status == "transaction committed");

// make sure that we cannot add an index
// during a multi statement transaction
db.jstests_txn_basic.insert({a:1});
t = db.runCommand({"beginTransaction":1});
assert(t.ok == 1);
assert(t.status == "transaction began");
db.jstests_txn_basic.ensureIndex({a:1});
t = db.runCommand({"getLastError":1});
assert(t.code == 16467);
t = db.runCommand({"commitTransaction":1});
assert(t.ok == 1);
assert(t.status == "transaction committed");

// simple test of committing a multi statement transaction that does an insert
t = db.jstests_txn_basic;
t.drop();
t.insert({a:"inserted before transaction create"});
s = db.runCommand({"beginTransaction":1});
assert(s.ok == 1);
s = t.insert({a:"inserted during transaction"});
assert(t.count() == 2);
s = t.find();
assert(s[1].a == "inserted during transaction");
s = db.runCommand({"rollbackTransaction":1});
assert(s.ok == 1);
assert(t.count() == 1);


// simple test of committing a multi statement transaction that does an insert
t = db.jstests_txn_basic;
t.drop();
t.insert({a:"inserted before transaction create"});
s = db.runCommand({"beginTransaction":1});
assert(s.ok == 1);
s = t.insert({a:"inserted during transaction"});
assert(t.count() == 2);
s = t.find();
assert(s[1].a == "inserted during transaction");
s = db.runCommand({"commitTransaction":1});
assert(s.ok == 1);
assert(t.count() == 2);
s = t.find();
assert(s[1].a == "inserted during transaction");
