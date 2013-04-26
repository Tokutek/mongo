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

// 5/2/2013 There's a bug with dropDatabase in a multi-statement transaction.
//          Due to time constraints, disable it and revisit the problem later.
begin();
assert.throws(db.dropDatabase());
rollback();
begin();
assert.throws(db.dropDatabase());
commit();

// This tests the feature, assuming it's enabled.
/*
db.a.insert({ a: 1 });
assert.eq(4, db.system.namespaces.count());
db.b.insert({ b: 1 });
assert.eq(6, db.system.namespaces.count());
db.b.ensureIndex({ b: 1 });
assert.eq(7, db.system.namespaces.count());
assert.eq(1, db.a.count());
assert.eq(1, db.b.count());
assert.eq(2, db.b.stats().nindexes);

// test that drop database rolls everything back on abort
begin();
db.dropDatabase();
assert.eq(0, db.system.namespaces.count());
assert.eq(0, db.a.count());
assert.eq(0, db.b.count());
rollback();
assert.eq(7, db.system.namespaces.count());
assert.eq(1, db.a.count());
assert.eq(1, db.b.count());
assert.eq(2, db.b.stats().nindexes);

// test that aborting drop database is ok when a collection
// was created in the same multi-statement transaction
begin();
db.c.insert({ c: 1 })
assert.eq(9, db.system.namespaces.count());
assert.eq(1, db.a.count());
assert.eq(1, db.b.count());
assert.eq(1, db.c.count());
rollback();
assert.eq(7, db.system.namespaces.count());
assert.eq(1, db.a.count());
assert.eq(1, db.b.count());
assert.eq(0, db.c.count());

// make sure we can do stuff to this database again, happily
db.c.insert({});
assert(!db.getLastError());
assert.eq(1, db.c.count());
db.f.insert({});
assert(!db.getLastError());
assert.eq(1, db.f.count());
db.c.drop();
assert(!db.getLastError());
db.f.drop();
assert(!db.getLastError());
db.dropDatabase();
*/
