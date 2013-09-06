// Simple test for index usage stats
t = db.indexusage;
t.drop();
t.insert({ a: 1, b: 1 });

function accessCount(name) {
    return t.stats()["indexDetails"].filter( function(o) { return o["name"] == name; })[0].accessCount;
}
assert.eq(0, accessCount("_id_"));

// We access the _id index to build secondary indexes, so the counter goes up.
t.ensureIndex({ a: 1 });
assert.eq(1, accessCount("_id_"));
assert.eq(0, accessCount("a_1"));
t.ensureIndex({ a: 1, b: 1 });
assert.eq(2, accessCount("_id_"));
assert.eq(0, accessCount("a_1"));
assert.eq(0, accessCount("a_1_b_1"));

assert.eq(2, accessCount("_id_"));
assert.eq(0, accessCount("a_1"));
assert.eq(0, accessCount("a_1_b_1"));
t.find({ c: 1 }).itcount();
assert.eq(3, accessCount("_id_"));
assert.eq(0, accessCount("a_1"));
assert.eq(0, accessCount("a_1_b_1"));
t.find({ a: 1, b: 5 }).hint({ a: 1, b: 1 }).itcount();
assert.eq(3, accessCount("_id_"));
assert.eq(0, accessCount("a_1"));
assert.eq(1, accessCount("a_1_b_1"));
t.find({ a: 1 }).hint({ a: 1, b: 1 }).itcount();
assert.eq(3, accessCount("_id_"));
assert.eq(0, accessCount("a_1"));
assert.eq(2, accessCount("a_1_b_1"));
t.find({ a: 1 }).hint({ a: 1 }).itcount();
assert.eq(3, accessCount("_id_"));
assert.eq(1, accessCount("a_1"));
assert.eq(2, accessCount("a_1_b_1"));

t.drop();
