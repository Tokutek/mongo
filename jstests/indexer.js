// Test that foreground and background index builds create special index types properly.

var testMultiKey = function(bg) {
    t = db.jstests_indexer_multikey;
    t.drop();
    t.insert({ a: [ 1, 2 ] });
    t.ensureIndex({ a: 1 }, { background: bg });
    assert.eq(1, t.count());
    assert.eq(true, t.find({ a: 1 }).hint({ a: 1 }).explain().isMultiKey);
};
testMultiKey(true);
testMultiKey(false);

var testSparse = function(bg) {
    t = db.jstests_indexer_sparse;
    t.drop();
    t.insert({ a: 1 });
    t.insert({ b: 1 });
    t.ensureIndex({ a: 1 }, { sparse: true, background: bg });
    assert.eq(2, t.count());
    assert.eq(1, t.find().sort({ a: 1 }).itcount());
}
testSparse(true);
testSparse(false);

var testHashed = function(bg) {
    t = db.jstests_indexer_hashed;
    t.drop();
    t.insert({ a: 1 });
    t.insert({ b: 1 });
    t.ensureIndex({ a: 'hashed' }, { background: bg });
    assert.eq(2, t.count());
    assert.eq("IndexCursor a_hashed", t.find({ a: 1 }).explain().cursor);

    t.drop();
    t.insert({ a: [ 1, 2 ] });
    t.insert({ b: 2 });
    // Cannot hash arrays
    t.ensureIndex({ a: 'hashed' }, { background: bg });
    assert( db.getLastError() );
    assert.eq(2, t.count());
    assert.eq("BasicCursor", t.find({ a: 1 }).explain().cursor);
}
testHashed(true);
testHashed(false);
