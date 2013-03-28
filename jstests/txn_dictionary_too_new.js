t = db.jstests_txn_isolation;

// test that cursor getting TOKUDB_DICTIONARY_TOO_NEW works
t.drop();
s = startParallelShell(' \
        t = db.jstests_txn_isolation; \
        sleep(1000); \
        t.insert({a:"during"}); \
');

x = db.runCommand("beginTransaction");
assert(x.ok == 1);
sleep(2000);
assert.throws(function(){t.count()});
x = db.runCommand("commitTransaction");
s();
