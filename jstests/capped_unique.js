// Test unique secondary indexes with capped collections
// This test exists because capped collections do not
// inherit unique check code from regular collections.
t = db.capped_unique;
t.drop();

// test with a non-unique secondary index

db.createCollection('capped_unique', { capped: true, size: 1024 } );
t.ensureIndex({ a: 1 });
assert(!db.getLastError());
t.insert({ a: 1 });
t.insert({ a: 1 });
assert(!db.getLastError());
assert(2, t.count());
assert(2, t.count({ a: 1}));

t.drop();

// test with a unique secondary index

db.createCollection('capped_unique', { capped: true, size: 1024 } );
assert.eq(null, db.getLastError());
t.ensureIndex({ a: 1 }, { unique: true });
assert.eq(null, db.getLastError());
t.insert({ a: 1 });
t.insert({ a: 1 });
assert(db.getLastError());
assert(1, t.count());
assert(1, t.count({ a: 1}));

// test with an ensured _id index, but without unique = true (forbidden)

t.drop();
db.createCollection('capped_unique', { capped: true, size: 1024, autoIndexId: false })
assert.eq(null, db.getLastError());
assert.throws(t.ensureIndex({ _id: 1 }));
assert.throws(t.ensureIndex({ _id: 1 }, { unique: false }));

// test with an ensured _id index, explicitly specifying unique = true

t.drop();
db.createCollection('capped_unique', { capped: true, size: 1024, autoIndexId: false })
assert.eq(null, db.getLastError());
t.ensureIndex({ _id: 1 }, { unique: true });
assert.eq(null, db.getLastError());
t.insert({ _id: 0 });
assert.eq(null, db.getLastError());
t.insert({ _id: 0 });
// This should fail
assert.neq(null, db.getLastError());
