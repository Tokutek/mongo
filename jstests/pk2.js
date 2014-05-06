// test that you cannot insert bad values for the primary key
t = db.pk2;
t.drop();

assert.commandWorked(db.runCommand({ create: 'pk2', primaryKey: { a: 1, b: 1, _id: 1 } }));
t.ensureIndex({ a: 1 });
t.ensureIndex({ b: 1 });

// array values (no multikey pk)
t.insert({ a: [ 1, 2 ] });
assert.neq(null, db.getLastError());
t.insert({ b: [ 1, 2 ] });
assert.neq(null, db.getLastError());
t.insert({ _id: [ 1, 2 ] });
assert.neq(null, db.getLastError());
t.insert({ c: 5, a: [ 1, 2 ] });
assert.neq(null, db.getLastError());

// regex
t.insert({ a: /hi/});
assert.neq(null, db.getLastError());
t.insert({ b: /hi/ });
assert.neq(null, db.getLastError());
t.insert({ _id: /hi/ });
assert.neq(null, db.getLastError());
t.insert({ c: 5, a: /hi/ });
assert.neq(null, db.getLastError());

// undefined
t.insert({ a: undefined});
assert.neq(null, db.getLastError());
t.insert({ b: undefined });
assert.neq(null, db.getLastError());
t.insert({ _id: undefined });
assert.neq(null, db.getLastError());
t.insert({ c: 5, a: undefined });
assert.neq(null, db.getLastError());

// through upsert
t.update({ _id: 0 }, { a: [ 1, 2 ] }, { upsert: true });
assert.neq(null, db.getLastError());
t.update({ _id: 0 }, { a: /hi/ }, { upsert: true });
assert.neq(null, db.getLastError());
t.update({ _id: 0 }, { a: undefined }, { upsert: true });
assert.neq(null, db.getLastError());

t.update({ _id: 0 }, { a: [ 1, 2 ] }, { upsert: true });
assert.neq(null, db.getLastError());
t.update({ _id: 0 }, { a: /hi/ }, { upsert: true });
assert.neq(null, db.getLastError());
t.update({ _id: 0 }, { a: undefined }, { upsert: true });
assert.neq(null, db.getLastError());

t.update({ _id: 0 }, { b: [ 1, 2 ] }, { upsert: true });
assert.neq(null, db.getLastError());
t.update({ _id: 0 }, { b: /hi/ }, { upsert: true });
assert.neq(null, db.getLastError());
t.update({ _id: 0 }, { b: undefined }, { upsert: true });
assert.neq(null, db.getLastError());

t.update({ _id: 0 }, { c: 5, a: [ 1, 2 ] }, { upsert: true });
assert.neq(null, db.getLastError());
t.update({ _id: 0 }, { c: 5, a: /hi/ }, { upsert: true });
assert.neq(null, db.getLastError());
t.update({ _id: 0 }, { c: 5, a: undefined }, { upsert: true });
assert.neq(null, db.getLastError());

t.update({ _id: 0 }, { _id: [ 1, 2 ] }, { upsert: true });
assert.neq(null, db.getLastError());
t.update({ _id: 0 }, { _id: /hi/ }, { upsert: true });
assert.neq(null, db.getLastError());
t.update({ _id: 0 }, { _id: undefined }, { upsert: true });
assert.neq(null, db.getLastError());
