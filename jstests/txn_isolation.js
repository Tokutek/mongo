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
r = t.find();
assert(r[1].a == "during");
r = db.runCommand({"commitTransaction":1});
s();

