// Make sure that multikey updates play nice with clustering indexes.
t = db.update_multikey_clustering;

var runTest = function(byId, buildIndexFirst) {
    t.drop();
    if (buildIndexFirst) {
        t.ensureIndex({ a: 1 }, { clustering: true });
        t.insert({ _id: 0, a: [ 1, 2, 3 ], c: "hello" });
    } else {
        t.insert({ _id: 0, a: [ 1, 2, 3 ], c: "hello" });
        t.ensureIndex({ a: 1 }, { clustering: true });
    }
    assert.eq(1, t.find({ a: 1, c: "hello" }).hint({ a: 1 }).itcount());
    assert.eq(1, t.find({ a: 1, c: "hello" }).hint({ $natural: 1 }).itcount());
    // Test both the regular and _updateById code paths
    t.update(byId ? { _id: 0 } : { c: "hello" }, { $set: { magic: 7 } }, { multi: false, upsert: false });
    // Should get the same results using a clustering index { a: 1 } versus a table scan.
    assert.eq(1, t.find({ a: 1, magic: 7 }).hint({ a: 1 }).itcount());
    assert.eq(1, t.find({ a: 2, magic: 7 }).hint({ a: 1 }).itcount());
    assert.eq(1, t.find({ a: 3, magic: 7 }).hint({ a: 1 }).itcount());
    assert.eq(1, t.find({ a: 1, magic: 7 }).hint({ $natural: 1 }).itcount());
    assert.eq(1, t.find({ a: 2, magic: 7 }).hint({ $natural: 1 }).itcount());
    assert.eq(1, t.find({ a: 3, magic: 7 }).hint({ $natural: 1 }).itcount());
};
runTest(true, true);
runTest(true, false);
runTest(false, true);
runTest(false, false);
