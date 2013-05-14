t = db.jstests_txn_dictionary_too_new;

// test that cursor getting TOKUDB_DICTIONARY_TOO_NEW works
t.drop();
s = startParallelShell(' \
        t = db.jstests_txn_dictionary_too_new; \
        sleep(1000); \
        t.insert({a:"during"}); \
');

x = db.runCommand("beginTransaction");
assert(x.ok == 1);
sleep(2000);
x = t.count();
assert.eq(x,0);
x = db.runCommand("commitTransaction");
s();
