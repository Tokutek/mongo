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
t = db.rollbackindex

// test that an index build can be rolled back
for (i = 0; i < 100; i++) {
    t.insert({ a: i });
}
assert.eq(100, t.count())
assert.eq(2, db.system.namespaces.find({ "name" : { $regex: "rollbackindex" } }).count());
begin();
t.ensureIndex({ a: 1});
assert(!db.getLastError());
assert.eq(3, db.system.namespaces.find({ "name" : { $regex: "rollbackindex" } }).count());
assert.eq(1, db.system.indexes.find({ "ns" : db.getName() + ".rollbackindex", "key" : { a : 1 } }).count())
assert.eq("IndexCursor a_1", t.find({ a: 50 }).hint({ a: 1 }).explain().cursor)
assert.eq(2, t.stats().nindexes)
rollback();
assert.eq(100, t.count()) // force the re-open
assert.eq(2, db.system.namespaces.find({ "name" : { $regex: "rollbackindex" } }).count());
assert.eq(0, db.system.indexes.find({ "ns" : db.getName() + ".rollbackindex", "key" : { a : 1 } }).count())
assert.throws(t.find({ a: 50 }).hint({ a: 1 })) // "bad hint"
assert.eq(1, t.stats().nindexes)

// test that we can happily recreate the index between begin/commit
begin();
assert.eq(2, db.system.namespaces.find({ "name" : { $regex: "rollbackindex" } }).count());
t.ensureIndex({ a: 1});
assert(!db.getLastError());
assert.eq(3, db.system.namespaces.find({ "name" : { $regex: "rollbackindex" } }).count());
assert.eq(1, db.system.indexes.find({ "ns" : db.getName() + ".rollbackindex", "key" : { a : 1 } }).count())
assert.eq(2, t.stats().nindexes)
assert.eq("IndexCursor a_1", t.find({ a: 50 }).hint({ a: 1 }).explain().cursor)
commit();
assert.eq(3, db.system.namespaces.find({ "name" : { $regex: "rollbackindex" } }).count());
assert.eq(1, db.system.indexes.find({ "ns" : db.getName() + ".rollbackindex", "key" : { a : 1 } }).count())
assert.eq(2, t.stats().nindexes)
assert.eq("IndexCursor a_1", t.find({ a: 50 }).hint({ a: 1 }).explain().cursor)
