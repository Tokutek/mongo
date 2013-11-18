// test that dropping an id or $_ index does not work
f = db.jstests_6155;
f.drop();
f.insert({a:1});
g = f.dropIndex('_id_');
assert.eq(0, g.ok);
h = f.count()
assert.eq(1, h);
f.drop();

// test that dropping a defined pk does not work
assert.commandWorked(db.runCommand({ create: 'jstests_6155', primaryKey: { x: 1, _id: 1 } }));
assert.eq(0, f.dropIndex({ x: 1,  _id: 1 }).ok);
f.drop();
