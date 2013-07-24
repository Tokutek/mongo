// Test that the primary key indexing locking is sane. When
// appending to the end if the _id keyspace, we shouldn't
// lock out the entire range from newKey -> maxKey.

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

t = db.txnpklocking;
t.drop();

t.insert({ _id: 0 });
t.insert({ _id: 1 });

// Walk a multi-statement txn side-by-side with other client shells inserting
// interleaved primary key values.
begin();
t.insert({ _id: 2 });
assert(!db.getLastError());
s1 = startParallelShell('db.txnpklocking.insert({ _id: 3 }); assert(!db.getLastError()); print("inserting magic 3"); db.txnpklocking.insert({ success: 3 })');
s1();
t.insert({ _id: 4 });
assert(!db.getLastError());
s2 = startParallelShell('db.txnpklocking.insert({ _id: 5 }); assert(!db.getLastError()); db.txnpklocking.insert({ success: 5 })');
s2();
t.insert({ _id: 6 });
assert(!db.getLastError());
commit();

// Make sure the other shells did not throw
assert.eq(1, t.count({ 'success': 3 }));
assert.eq(1, t.count({ 'success': 5 }));

// Do the same thing but auto generate the primary keys.
begin();
for (i = 0; i < 10; i++) {
    startParallelShell('db.txnpklocking.insert({}); assert(!db.getLastError());')();
    t.insert({});
    assert(!db.getLastError());
}
commit();
