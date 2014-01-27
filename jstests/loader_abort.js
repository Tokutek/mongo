// Test that committing a load leaves the collection in the desired state.

var filename;
if (TestData.testDir !== undefined) {
    load(TestData.testDir + "/_loader_helpers.js");
} else {
    load('jstests/_loader_helpers.js');
}

var testIndexedInsertAbort = function() {
    t = db.loaderindexedinsertabort;
    t.drop();
    begin();
    beginLoad('loaderindexedinsertabort', [ { key: { a: 1 }, name: "a_1" } ], { } );
    t.insert({ a: 700 } );
    abortLoad();
    commit();
    assert.eq(0, t.count({ a: 700 }));
    // No index on a: 1 since we aborted the load (the hint should uassert)
    assert.throws(t.find({a : 700}).hint({ a: 1 }).itcount());
}();

var testUsableAfterAbort = function() {
    t = db.loaderusableabort;
    t.drop();
    begin();
    beginLoad('loaderusableabort', [ ], { });
    t.insert({ duringLoad: 1 });
    assert(!db.getLastError());
    abortLoad();
    rollback();
    assert.eq(0, t.count());
    assert.eq(0, t.count({ duringLoad: 1 }));

    t.insert({ afterLoad: 1 });
    assert.eq(1, t.count());
    assert.eq(0, t.count({ duringLoad: 1 }));
    assert.eq(1, t.count({ afterLoad: 1 }));
    t.remove({ afterLoad: 1 });
    assert.eq(0, t.count());
}();

var testExternallyUsableAfterAbort = function() {
    t = db.loaderusableabort1;
    t.drop();
    begin();
    beginLoad('loaderusableabort1', [ ], { });
    t.insert({ duringLoad: 1 });
    assert(!db.getLastError());
    abortLoad();
    rollback();
    assert.eq(0, t.count());
    assert.eq(0, t.count({ duringLoad: 1 }));

    s = startParallelShell('k = db.loaderusableabort1.count(); assert.eq(0, k); db.loaderusableabort1.insert({ success: 1 }); assert(!db.getLastError());')
    s();
    assert.eq(1, t.count({ success: 1 }));
}();

var testUniquenessConstraintAbort = function() {
    t = db.loaderunique;
    t.drop();
    begin();
    beginLoad('loaderunique', [ { key: { a: 1 }, name: "a_1", unique: true } ], { } );
    t.insert({ a: 1 });
    assert(!db.getLastError());
    t.insert({ a: 2 });
    assert(!db.getLastError());
    t.insert({ a: 2 });
    assert(!db.getLastError());
    abortLoad();
    commit();
    assert.eq(0, t.count());

    // Test rollback (should be the same result)
    t.drop();
    begin();
    beginLoad('loaderunique', [ { key: { a: 1 }, name: "a_1", unique: true } ], { } );
    t.insert({ _id: 1 });
    assert(!db.getLastError());
    t.insert({ _id: 2 });
    assert(!db.getLastError());
    t.insert({ _id: 2 });
    assert(!db.getLastError());
    abortLoad();
    rollback();
    assert.eq(0, t.count());
}();
