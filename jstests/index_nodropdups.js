// test that drop dups is stripped from the index spec
// and does not actually drop duplicates.

t = db.nodropdups;
t.drop();

t.insert({ a: 1 });
t.insert({ a: 1 });

// unique index on { a: 1 } fails
t.ensureIndex({ a: 1 }, { unique: true, dropDups: true });
assert.neq(null, db.getLastError());
assert.eq(2, t.find().itcount());
assert.eq(2, t.find({ a: 1 }).itcount());

// remove one of the { a: 1 } docs and then it succeeds
t.remove({ a: 1 }, { justOne: true });
t.ensureIndex({ a: 1 }, { unique: true, dropDups: true });
assert.eq(null, db.getLastError());
// index should exist and have not have dropDups in the spec
assert.eq({ a: 1 }, t.getIndexes()[1].key);
assert.eq(true, t.getIndexes()[1].unique);
assert.eq(undefined, t.getIndexes()[1].dropDups);
assert.eq(true, db.system.indexes.find({ ns: db.getName() + '.nodropdups', key: { a: 1 } })[0].unique);
assert.eq(undefined, db.system.indexes.find({ ns: db.getName() + '.nodropdups', key: { a: 1 } })[0].dropDups);
    
