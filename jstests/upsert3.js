// #736
// should not be able to insert bad _id values using upsert

t = db.upsertbad;
t.drop();

t.update({ _id: /regex/ }, { _id: /regex/ }, { upsert: true});
assert.neq(null, db.getLastError());
t.update({ _id: [ 1, 2 ] }, { _id: [ 1, 2 ] }, { upsert: true});
assert.neq(null, db.getLastError());
t.update({ _id: undefined }, { _id: undefined }, { upsert: true});
assert.neq(null, db.getLastError());

t.insert({ a: 1 });
t.ensureIndex({ a: 1 });

t.update({ a: 1 }, { _id: /regex/ }, { upsert: true});
assert.neq(null, db.getLastError());
t.update({ a: 1 }, { _id: [ 1, 2 ] }, { upsert: true});
assert.neq(null, db.getLastError());
t.update({ a: 1 }, { _id: undefined }, { upsert: true});
assert.neq(null, db.getLastError());
