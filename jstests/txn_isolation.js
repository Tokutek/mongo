t = db.jstests_txn_isolation;

// test that a simple MVCC read works right
// parallel thread does a provisional insert but does not commit
// main thread verifies that it does not see the provisional insert
t.drop();
t.insert({a:"before"});
s = startParallelShell(' \
        t = db.jstests_txn_isolation; \
        r = db.runCommand("beginTransaction"); \
        t.insert({a:"during"}); \
        sleep(2000); \
        r = db.runCommand({"commitTransaction":1}); \
        assert(r.ok == 1);      \
');

sleep(1000);
count = t.count();
assert(count == 1);
s();

// simple read uncommitted test
t.drop();
t.insert({a:"before"});
s = startParallelShell(' \
        t = db.jstests_txn_isolation; \
        r = db.runCommand("beginTransaction", {"isolation":"readUncommitted"}); \
        t.insert({a:"during"}); \
        sleep(2000); \
        r = db.runCommand({"commitTransaction":1}); \
        assert(r.ok == 1);      \
');

sleep(1000);
r = db.runCommand({"beginTransaction":1, "isolation":"readUncommitted"});
count = t.count();
assert(count == 2);
r = db.runCommand({"commitTransaction":1});
s();

// simple serializable test
t.drop();
t.insert({a:"before"});
s = startParallelShell(' \
        t = db.jstests_txn_isolation; \
        sleep(1000); \
        r = db.runCommand("beginTransaction"); \
        t.insert({a:"during"}); \
        r = db.runCommand({"commitTransaction":1}); \
        assert(r.ok == 1);      \
');

r = db.runCommand({"beginTransaction":1, "isolation":"serializable"});
sleep(2000);
count = t.count();
assert(count == 2);
r = t.find();
assert(r[1].a == "during");
r = db.runCommand({"commitTransaction":1});
s();

// simple lock wait timeout test
t.drop();
t.insert({a:"before"});
s = startParallelShell(' \
        t = db.jstests_txn_isolation; \
        r = db.runCommand("beginTransaction"); \
        t.insert({a:"during"}); \
        sleep(10000); \
        r = db.runCommand({"commitTransaction":1}); \
        assert(r.ok == 1);      \
');

r = db.runCommand({"beginTransaction":1, "isolation":"serializable"});
sleep(1000);
assert.throws(function(){t.count()});
r = db.runCommand({"commitTransaction":1});
s();

// lock wait timeout test after changing timeout
t.drop();
t.insert({a:"before"});
s = startParallelShell(' \
        t = db.jstests_txn_isolation; \
        r = db.runCommand("beginTransaction"); \
        t.insert({a:"during"}); \
        sleep(10000); \
        r = db.runCommand({"commitTransaction":1}); \
        assert(r.ok == 1);      \
');

r = db.runCommand({setClientLockTimeout: 8000});
assert.commandWorked(r);
var oldTimeout = r.was;
assert.commandWorked(db.runCommand({"beginTransaction":1, "isolation":"serializable"}));
sleep(1000);
assert.throws(function(){t.count()});
assert.commandWorked(db.runCommand({"commitTransaction":1}));
assert.commandWorked(db.runCommand({setClientLockTimeout: oldTimeout}));
s();

// lock wait timeout test after changing timeout part 2
t.drop();
t.insert({a:"before"});
s = startParallelShell(' \
        t = db.jstests_txn_isolation; \
        r = db.runCommand("beginTransaction"); \
        t.insert({a:"during"}); \
        sleep(10000); \
        r = db.runCommand({"commitTransaction":1}); \
        assert(r.ok == 1);      \
');

r = db.runCommand({setClientLockTimeout: 12000});
assert.commandWorked(r);
var oldTimeout = r.was;
sleep(1000);
assert.commandWorked(db.runCommand({"beginTransaction":1, "isolation":"serializable"}));
assert.eq(t.count(), 2);
assert.commandWorked(db.runCommand({"commitTransaction":1}));
assert.commandWorked(db.runCommand({setClientLockTimeout: oldTimeout}));
s();

// simple dictionary too new test
t.drop();
r = db.runCommand({"beginTransaction":1, "isolation":"mvcc"});
s = startParallelShell(' \
        t = db.jstests_txn_isolation; \
        t.insert({a:"during"}); \
        sleep(10000); \
');

sleep(1000);
r = t.count();
assert(r == 0);
r = db.runCommand({"commitTransaction":1});
s();
