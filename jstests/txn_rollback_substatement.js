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

db.dropDatabase();
t = db.rollbacksubstatement;

// test that a failed substatement does not roll back the whole transaction.
t.insert({ _id: 1 });
begin();
t.insert({ _id: 2 });
assert.throws(t.insert({ _id: 1 })); // oops!
t.insert({ _id: 0 });
commit();
assert.eq(1, t.count({ _id: 0 }));
assert.eq(1, t.count({ _id: 1 }));
assert.eq(1, t.count({ _id: 2 }));

// test that a failed substatement does not roll back capped collection inserts
t.drop();
db.createCollection('t', { capped: true, size: 1024 });
begin();
t.insert({ _id: 0 });
t.insert({ _id: 2 });
expected = t.stats();
function verify() {
    st = t.stats();
    assert.eq( expected.cappedSizeCurrent, st.cappedSizeCurrent ) // should not have changed
    assert.eq( expected.cappedCount, st.cappedCount ) // should not have changed
    assert.eq(2, t.count());
    assert.eq(1, t.count({ _id: 0 }));
    assert.eq(1, t.count({ _id: 2 }));
}
verify();
assert.throws(t.insert({ _id: 0 })); // oops!
verify();
commit();
verify();

// test that a failed substatement does not trigger whole-transaction rollback, with fileops
t.drop();
t.insert({ _id: 1 });
begin();
t.drop();
t.insert({ _id: 0 });
t.insert({ _id: 2 });
assert.throws(t.insert({ _id: 0 })); // oops!
assert.eq(2, t.count());
assert.eq(1, t.count({ _id: 0 }));
assert.eq(1, t.count({ _id: 2 }));
rollback();
assert.eq(1, t.count());
assert.eq(1, t.count({ _id: 1 }));
