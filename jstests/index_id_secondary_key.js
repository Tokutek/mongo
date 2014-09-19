t = db.idsecondarykey;
t.drop();

db.createCollection('idsecondarykey', { primaryKey: { a: 1, _id: 1 } });
assert.eq({ a: 1, _id: 1 }, t.getIndexes()[0].key);
t.insert({ _id: 5 });
t.insert({ _id: 3 });
t.insert({ _id: 7 });
// the sort order should be correct
assert.eq([ { _id: 3 }, { _id: 5 }, { _id: 7 } ], t.find().toArray());

// 5 3 and 7 are already in the index
t.insert({ _id: 5 });
assert.neq(null, db.getLastError());
t.insert({ _id: 3 });
assert.neq(null, db.getLastError());
t.insert({ _id: 7 });
assert.neq(null, db.getLastError());

// 6 is ok
t.insert({ _id: 6 });
assert.eq(4, t.count());

