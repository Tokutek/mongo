// Test that a primary key may be specified

t = db.pk;
t.drop();

assert.commandWorked(db.runCommand({ create: 'pk', primaryKey: { a: 1, _id: 1 } }));
firstIndex = t.getIndexes()[0];
secondIndex = t.getIndexes()[1];
assert.eq({ a: 1, _id: 1 }, firstIndex.key);
assert.eq(true, firstIndex.clustering);
assert.eq(true, firstIndex.unique);
assert.eq({ _id: 1 }, secondIndex.key);
assert.eq(false, secondIndex.clustering ? true : false);
assert.eq(true, secondIndex.unique ? true : false);

// provide just 'a'
// generated pk is { 1, generatedId }
t.insert({ a: 1 });
t.insert({ a: 1 });
assert.eq(2, t.find({ a: 1 }).itcount());

// provide both _id and a
// generated pk is { 10, 100 }
t.insert({ a: 10, _id: 100 });
assert.eq(1, t.find({ a: 10 }).itcount());
assert.eq(1, t.find({ a: 10, _id: 100 }).itcount());

// do not provide an _id field.
// generated pk is { null, generatedId }
t.remove({});
t.insert({});
assert.eq(1, t.find().itcount());

// provide an _id field.
// generated pk is { null, 0 } which is ok.
t.insert({ _id: 0 });
assert.eq(1, t.find({ _id: 0 }).itcount());
assert.eq(1, t.find({ a: null, _id: 0 }).itcount());

// generate a single secondary key and do a lookup
t.ensureIndex({ b: 1 });
t.insert({ b: 1 });
assert.eq(1, t.find({ b: 1 }).hint({ b: 1 }).itcount());
