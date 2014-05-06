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
t = db.rollbackdropindex;

for (i = 0; i < 100; i++) {
    t.insert({ a: i , b: i });
}
assert.eq(100, t.count());
assert.eq(2, db.system.namespaces.find({ "name" : { $regex: "rollbackdropindex" } }).count());
t.ensureIndex({ a: 1 });
assert(!db.getLastError());
assert.eq(3, db.system.namespaces.find({ "name" : { $regex: "rollbackdropindex" } }).count());
assert.eq(1, db.system.indexes.find({ "ns" : db.getName() + ".rollbackdropindex", "key" : { a : 1 } }).count());
assert.eq(2, t.stats().nindexes);
assert.eq("IndexCursor a_1", t.find({ a: 50 }).hint({ a: 1 }).explain().cursor);

// test that an index drop can be rolled back
begin();
r = t.dropIndex({ a: 1 });
assert.eq(r.nIndexesWas, 2);
assert(!db.getLastError());
assert.eq(2, db.system.namespaces.find({ "name" : { $regex: "rollbackdropindex" } }).count());
assert.eq(0, db.system.indexes.find({ "ns" : db.getName() + ".rollbackdropindex", "key" : { a : 1 } }).count())
assert.throws(t.find({ a: 50 }).hint({ a: 1 })) // "bad hint"
assert.eq(1, t.stats().nindexes)
rollback();
assert.eq(100, t.count()) // force the re-open
assert(!db.getLastError());
assert.eq(3, db.system.namespaces.find({ "name" : { $regex: "rollbackdropindex" } }).count());
assert.eq(1, db.system.indexes.find({ "ns" : db.getName() + ".rollbackdropindex", "key" : { a : 1 } }).count());
assert.eq(2, t.stats().nindexes);
assert.eq("IndexCursor a_1", t.find({ a: 50 }).hint({ a: 1 }).explain().cursor);

// test that to indexes can be dropped together
t.ensureIndex({ b: 1} );
assert(!db.getLastError());
assert.eq(4, db.system.namespaces.find({ "name" : { $regex: "rollbackdropindex" } }).count());
assert.eq(1, db.system.indexes.find({ "ns" : db.getName() + ".rollbackdropindex", "key" : { b : 1 } }).count());
assert.eq("IndexCursor b_1", t.find({ b: 50 }).hint({ b: 1 }).explain().cursor);
assert.eq(3, t.stats().nindexes);

begin();
r = t.dropIndexes()
assert.eq(r.nIndexesWas, 3);
assert(!db.getLastError());
commit();
assert(!db.getLastError());
assert.eq(2, db.system.namespaces.find({ "name" : { $regex: "rollbackdropindex" } }).count());
assert.eq(0, db.system.indexes.find({ "ns" : db.getName() + ".rollbackdropindex", "key" : { a : 1 } }).count())
assert.eq(0, db.system.indexes.find({ "ns" : db.getName() + ".rollbackdropindex", "key" : { b : 1 } }).count())
assert.eq(1, t.stats().nindexes)
assert.throws(t.find({ a: 50 }).hint({ a: 1 })) // "bad hint"
assert.throws(t.find({ b: 50 }).hint({ b: 1 })) // "bad hint"
