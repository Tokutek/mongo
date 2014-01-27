// Test that special indexes are recognized during load.

var filename;
if (TestData.testDir !== undefined) {
    load(TestData.testDir + "/_loader_helpers.js");
} else {
    load('jstests/_loader_helpers.js');
}

var testSparse = function() {
    t = db.loadersparse;
    t.drop();
    begin();
    beginLoad('loadersparse', [ { key: { a: 1 }, name: "a_1", sparse: true }, ], { });
    t.insert({ a: 1 });
    t.insert({ b: 1 });
    t.insert({ c: 1 });
    t.insert({ a: 2 });
    commitLoad();
    commit();
    assert.eq(true, t.getIndexes()[1].sparse);
    assert.eq(4, t.count());
    assert.eq(2, t.find().sort( { a : 1 } ).itcount() , "scan on sparse index a_1" )
    assert.eq(4, t.find().sort( { _id : 1 } ).itcount() , "scan on non-sparse index id_1" )

    t.drop();
    begin();
    beginLoad('loadersparse', [ { key: { e: 1 }, name: "a_1", sparse: true }, ], { });
    t.insert({ a: 1 });
    t.insert({ b: 1 });
    t.insert({ c: 1 });
    t.insert({ a: 2 });
    commitLoad();
    commit();
    assert.eq(true, t.getIndexes()[1].sparse);
    assert.eq(4, t.count());
    assert.eq(0, t.find().sort( { e : 1 } ).itcount() , "scan on sparse index a_1" );
    assert.eq(4, t.find().sort( { _id : 1 } ).itcount() , "scan on non-sparse index id_1" );
}();

var testMultiKey = function() {
    t = db.loadermultikey;
    t.drop();
    begin();
    beginLoad('loadermultikey', [ { key: { a: 1 }, name: "a_1" } ], { });
    t.insert({ a: [ 1, 2, 4, 5 ] });
    t.insert({ b: 2 });
    commitLoad();
    commit();
    assert.eq(true, t.find({ a: 1 }).hint({ a: 1 }).explain().isMultiKey);
    assert.eq(2, t.count());
    assert.eq(1, t.find({ a: 1 }).hint({ a: 1 }).itcount(), "finding 1" );
    assert.eq(1, t.find({ a: 2 }).hint({ a: 1 }).itcount(), "finding 2" );
    assert.eq(1, t.find({ a: 4 }).hint({ a: 1 }).itcount(), "finding 4" );
    assert.eq(1, t.find({ a: 5 }).hint({ a: 1 }).itcount(), "finding 5" );
}();

var testNotMultiKey = function() {
    t = db.loadernotmultikey;
    t.drop();
    begin();
    beginLoad('loadernotmultikey', [ { key: { a: 1 }, name: "a_1" } ], { });
    t.insert({ a: 1 });
    t.insert({ b: 2 });
    commitLoad();
    commit();
    assert.eq(false, t.find({ a: 1 }).hint({ a: 1 }).explain().isMultiKey);
    assert.eq(2, t.count());
    assert.eq(1, t.find({ a: 1 }).hint({ a: 1 }).itcount(), "finding 1" );
    assert.eq(1, t.find({ b: 2 }).hint({ a: 1 }).itcount(), "finding 2" );
}();

var testHashed = function() {
    t = db.loaderhashedidx;
    t.drop();
    begin();
    beginLoad('loaderhashedidx', [ { key: { a: 'hashed' }, name: "a_hashed" } ], { });
    t.insert({ a: 1 });
    t.insert({ a: 10 });
    t.insert({ b: 2 });
    commitLoad();
    commit();
    assert.eq(3, t.count());
    assert.eq(1, t.find({ a: 1 }).hint({ a: 'hashed' }).itcount(), "finding 1" );
    assert.eq(1, t.find({ a: 10 }).hint({ a: 'hashed' }).itcount(), "finding 10" );

    t.drop();
    begin();
    beginLoad('loaderhashedidx', [ { key: { a: 'hashed' }, name: "a_hashed" } ], { });
    // Cannot hash arrays
    t.insert({ a: [ 1, 10 ] });
    t.insert({ b: 2 });
    commitLoadShouldFail();
    commit();
    assert.eq(0, t.count());
}();

var testAmbiguousFieldNames = function() {
    t = db.loaderambiguousfieldnames;
    t.drop();
    begin();
    beginLoad('loaderambiguousfieldnames', [ { key: { 'a.0': 1 }, name: "a_1" } ], { });
    t.insert( {a:[{'0':[{'0':1}]}]} );
    commitLoadShouldFail();
    commit();
    assert.eq(0, t.count());
}();
