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

// Test that a multi-statement transaction can create and use
// a cursor that reads more than one batchSize() and also
// does writes to the same and other collections.

t = db.txncursor;

function checkLiveCursors(n) {
    s = db.serverStatus()['cursors'];
    assert.eq(n, s.clientCursors_size, 'cursor status: ' + tojson(s));
}

function runTest(doCommit) {
    db.dropDatabase();

    for ( i = 0; i < 1500; i++ ) {
        t.insert({ _id: i });
    }

    begin();
    c1 = t.find().batchSize(50);
    assert(c1.hasNext());
    checkLiveCursors(1);
    count = 1500;
    while (c1.hasNext() && count--) {
        o = c1.next();
        t.insert({ duringCursorIterate : 1 });
        db.unrelated.insert(o);
    }

    c2 = t.find({ duringCursorIterate : 1 }).batchSize(50);
    assert(c2.hasNext());
    checkLiveCursors(2); // c1 is still not exhausted, because we did inserts into t
    count = 0;
    while (c2.hasNext()) {
        c2.next();
        count++;
    }
    checkLiveCursors(1); // only c1 is left alive
    assert.eq(count, 1500);

    count = 0;
    c3 = db.unrelated.find().batchSize(50);
    assert(c3.hasNext());
    checkLiveCursors(2); // c1 and c3 are alive
    while (c3.hasNext()) {
        c3.next();
        count++;
    }
    checkLiveCursors(1); // just c1 is alive
    assert.eq(count, 1500);

    // commit or abort while c1 is still alive
    if (doCommit) {
        commit();
    } else {
        rollback();
        assert.eq(1500, t.count());
        assert.eq(0, db.unrelated.count());
    }
}

runTest(true);
runTest(false);
