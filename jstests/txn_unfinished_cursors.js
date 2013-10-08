function begin() {
    db.runCommand({ 'beginTransaction': 1 });
    assert(!db.getLastError());
}
function commit() {
    db.runCommand({ 'commitTransaction': 1 });
    assert(!db.getLastError());
}
function rollback() {
    db.runCommand({ 'rollbackTransaction': 1 });
    assert(!db.getLastError());
}

// Test that a multi-statement transaction can create several
// cursors, only partially iterate through the results, and
// then commit/abort.

db.dropDatabase();

t = db.txnunfinishedcursors;

for ( i = 0; i < 15000; i++ ) {
    t.insert({ _id: i });
}
assert.eq(15000, t.count());

function checkLiveCursors(n) {
    s = db.serverStatus()['cursors'];
    assert.eq(n, s.clientCursors_size, 'cursor status: ' + tojson(s));
}

function runTest(doCommit) {
    checkLiveCursors(0);

    begin();
    c1 = t.find().batchSize(50);
    c2 = t.find({ _id: { $lt: 500 } }).batchSize(50);
    assert(c1.hasNext());
    assert(c2.hasNext());

    checkLiveCursors(2);

    count = 0;
    while (1) {
        if (c2.hasNext()) {
            assert(c1.hasNext(), 'c1 exhausted early, count ' + count);
            c1.next();
            c2.next();
            count++;
        } else {
            break;
        }
    }
    assert.eq(count, 500);

    checkLiveCursors(1);

    if (doCommit) {
        commit();
    } else {
        rollback();
    }

    checkLiveCursors(0);
}

runTest(true);
runTest(false);
