// Test secondary indexes with a defined primary key

t = db.pk1;
t.drop();

db.runCommand({ create: 'pk1', primaryKey: { a: 1, b: 1, _id: 1 } });
t.ensureIndex({ c: 1 });
for (i = 0; i < 100; i++) {
    t.insert({ a: i, b: 100 - i, c: i + 100 });
}
assert.eq(100, t.count());
assert.eq(1, t.find({ c: 150 }).hint({ c: 1 }).itcount());
assert.eq(75, t.find({ c: { $gte: 125 } }).itcount());
assert.eq(0, t.find({ c: -700 }).itcount());
assert.eq(0, t.find({ c: 700 }).itcount());
for (i = 0; i < 100; i++) {
    t.insert({ c: 1000 + i });
}
assert.eq(200, t.count());
assert.eq(1, t.find({ c: 1000 }).itcount());
assert.eq(0, t.find({ c: 700 }).itcount());

t.remove({ c: 1 });
assert.eq(0, t.find({ c: 1 }).itcount());
