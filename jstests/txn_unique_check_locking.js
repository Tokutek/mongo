// Test that the primary key indexing locking is sane. When
// appending to the end if the _id keyspace, we shouldn't
// lock out the entire range from newKey -> maxKey.

t = db.txnuniquechecklocking;
t.drop();
t.ensureIndex({ a: 1 }, { unique: true });
assert.eq(null, db.getLastError());

var runTest = function(firstClientInserts, secondClientOp, secondClientObj) {
    t.remove();
    t.insert({ _id: 10, a: 10 });
    t.insert({ _id: 20, a: 20 });

    assert.commandWorked(db.beginTransaction());
    t.insert(firstClientInserts);
    s1 = startParallelShell('db.txnuniquechecklocking.' +
                                (secondClientOp == 'insert' ? 'insert(' : 'remove(') + tojson(secondClientObj) + ');' +
                            'assert.eq(null, db.getLastError());' +
                            'db.txnuniquechecklocking.insert({ _id: "success" });' +
                            'assert.eq(null, db.getLastError());'
                            );
    s1();
    assert.commandWorked(db.commitTransaction());
    assert.eq(1, t.count({ _id: 'success' }));
}

// Given unique index on 'a' with index rows:
//  [ 10, 20 ]
// We should be able to insert the first argument and, concurrently
// in another client, insert or remove the second argument (that is,
// we should be able to acquire the row locks we need.
runTest({ a: 5 }, 'insert', { a: 4 });
runTest({ a: 5 }, 'insert', { a: 6 });
runTest({ a: 5 }, 'remove', { _id: 10 });
runTest({ a: 5 }, 'remove', { _id: 20 });
runTest({ a: 10 }, 'insert', { a: 9 });
runTest({ a: 10 }, 'insert', { a: 11 });
runTest({ a: 10 }, 'remove', { _id: 20 });
runTest({ a: 15 }, 'insert', { a: 14 });
runTest({ a: 15 }, 'insert', { a: 16 });
runTest({ a: 15 }, 'remove', { _id: 10 });
runTest({ a: 15 }, 'remove', { _id: 20 });
runTest({ a: 20 }, 'insert', { a: 19 });
runTest({ a: 20 }, 'insert', { a: 21 });
runTest({ a: 20 }, 'remove', { _id: 10 });
