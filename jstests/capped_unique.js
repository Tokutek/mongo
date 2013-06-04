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
t.ensureIndex({ a: 1 }, { unique: true });
assert(!db.getLastError());
t.insert({ a: 1 });
t.insert({ a: 1 });
assert(db.getLastError());
assert(1, t.count());
assert(1, t.count({ a: 1}));
