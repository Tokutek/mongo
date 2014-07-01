// test a fix for a bug where we'd write a long to a compound
// index, but later interpret the bits as a double

t = db.idxbiglong;
t.drop();
t.ensureIndex({ a: 1, b: 1 });

x = NumberLong("9223372036854775807");
t.insert({ a: x });
assert.eq(1, t.count());
assert.eq(1, t.count({ a: x }));
assert.eq(1, t.find({ a: x }).itcount());
assert.eq(x, t.findOne({ a: x }).a);

// another bad ordering case
t.drop();
t.ensureIndex({ a: 1, b: 1 });
x = NumberLong("9007199254740990");
t.insert({ a: x + 3 });
t.insert({ a: x });
assert.eq(2, t.count());
assert.eq(1, t.find({ a: x + 3 }).itcount());
assert.eq(1, t.find({ a: x }).itcount());
vals = t.find().sort({ a: 1 }).toArray();
// should be properly sorted
assert.eq(vals[0].a, x, "vals[0].a bad " + vals[0].a);
assert.eq(vals[1].a, x + 3, "vals[1].a bad " + vals[1].a);
