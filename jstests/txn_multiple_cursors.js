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
// several cursors at once, interleaved.

t = db.txnmultiplecursors;
t.drop();
for ( i = 0; i < 1500; i++ ) {
    t.insert({ _id: i });
}

function runTest(doCommit) {
    begin();
    c1 = t.find().batchSize(50);
    c2 = t.find({ _id: { $gte: 500 } }).batchSize(50);
    c3 = t.find({ _id: { $lt : 1000  } }).batchSize(50);

    count = 0;
    while (1) {
        a = c1.hasNext();
        b = c2.hasNext();
        c = c3.hasNext();
        if (a) {
            count++;
            c1.next();
        }
        if (b) {
            count++;
            c2.next();
        }
        if (c) {
            count++;
            c3.next();
        }
        if (!a && !b && !c) {
            break;
        }
    }
    assert.eq(count, 3500);
    if (doCommit) {
        commit();
    } else {
        rollback();
    }
}

runTest(true);
runTest(false);
