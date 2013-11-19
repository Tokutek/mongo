// Test that updates work with a defined primary key
t = db.pkupdate;
t.drop();

assert.commandWorked(db.runCommand({ create: 'pkupdate', primaryKey: { c: 1, _id: 1 } }));
t.ensureIndex({ b: 1 });
t.insert({ b: 1, c: 1 });
assert.eq(1, t.find({ b: 1 }).hint({ b: 1 }).itcount());
assert.eq(1, t.find({ c: 1 }).hint({ c: 1, _id: 1 }).itcount());
t.update({ b: 1 }, { $set : { c: 2 } });
assert.eq(1, t.find({ b: 1 }).hint({ b: 1 }).itcount());
assert.eq(1, t.find({ c: 2 }).hint({ c: 1, _id: 1 }).itcount());
t.update({ c: 2 }, { $set : { z: 1 } });
assert.eq(1, t.find({ b: 1 }).hint({ b: 1 }).itcount());
assert.eq(1, t.find({ c: 2 }).hint({ c: 1, _id: 1 }).itcount());
assert.eq(1, t.find({ z: 1 }).itcount());
t.update({ c: 2 }, { $set : { c: 0 } });
assert.eq(0, t.find({ c: 2 }).hint({ b: 1 }).itcount());
assert.eq(0, t.find({ c: 2 }).hint({ c: 1, _id: 1 }).itcount());
assert.eq(1, t.find({ c: 0 }).hint({ b: 1 }).itcount());
assert.eq(1, t.find({ c: 0 }).hint({ c: 1, _id: 1 }).itcount());
