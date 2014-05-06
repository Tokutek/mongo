// Test that committing a load leaves the collection in the desired state.

var filename;
if (TestData.testDir !== undefined) {
    load(TestData.testDir + "/_loader_helpers.js");
} else {
    load('jstests/_loader_helpers.js');
}

var testValidOptions = function() {
    t = db.loadvalidoptions;
    t.drop();

    // Test default options
    begin();
    beginLoad('loadvalidoptions', [ ] , { });
    commitLoad();
    commit();
    assert.eq(null, db.system.namespaces.findOne({ 'name' : db.getName() + '.loadvalidoptions' }).options);
    assert.eq(null, t.getIndexes()[0].compression);
    assert.eq('zlib', t.stats().indexDetails[0].compression);

    // Test non-default options
    t.drop();
    begin();
    options = { compression: 'lzma', somethingSpecial: 'special' };
    beginLoad('loadvalidoptions', [ ] , options);
    commitLoad();
    commit();
    assert.eq(options, db.system.namespaces.findOne({ 'name' : db.getName() + '.loadvalidoptions' }).options);
    assert.eq(options.compression, t.getIndexes()[0].compression);
    assert.eq(options.compression, t.stats().indexDetails[0].compression);
}();

var testValidIndex = function() {
    t = db.loadvalidindexes;
    t.drop();
    begin();
    beginLoad('loadvalidindexes', [ { key: { a: 1 }, ns: db.getName() + '.loadvalidindexes', name: 'a_1' } ], { });
    assert.eq({ _id: 1 }, t.getIndexes()[0].key);
    assert.eq({ a: 1 }, t.getIndexes()[1].key);
    assert.eq(db.getName() + '.loadvalidindexes', t.getIndexes()[1].ns);
    assert.eq('a_1', t.getIndexes()[1].name);
    commitLoad();
    commit();
}();

var testValidIndexes = function() {
    t = db.loadvalidindexes;
    t.drop();
    begin();
    beginLoad('loadvalidindexes',
            [
                { key: { a: 1 }, ns: db.getName() + '.loadvalidindexes', name: 'a_1' },
                { key: { c: 1 }, ns: db.getName() + '.loadvalidindexes', name: 'c_1', clustering: true },
                { key: { a: 'hashed' }, ns: db.getName() + '.loadvalidindexes', name: 'a_hashed' },
                { key: { a: -1, b: -1, c: 1 }, ns: db.getName() + '.loadvalidindexes', name: 'a_-1_b-1_c_1', basement_size: 1024 }
            ], { });
    commitLoad();
    commit();
    assert.eq({ _id: 1 }, t.getIndexes()[0].key);
    assert.eq({ a: 1 }, t.getIndexes()[1].key);
    assert.eq(db.getName() + '.loadvalidindexes', t.getIndexes()[1].ns);
    assert.eq('a_1', t.getIndexes()[1].name);

    assert.eq({ c: 1 }, t.getIndexes()[2].key);
    assert.eq(db.getName() + '.loadvalidindexes', t.getIndexes()[2].ns);
    assert.eq('c_1', t.getIndexes()[2].name);
    assert.eq(true, t.getIndexes()[2].clustering);

    assert.eq({ a: 'hashed' }, t.getIndexes()[3].key);
    assert.eq(db.getName() + '.loadvalidindexes', t.getIndexes()[3].ns);
    assert.eq('a_hashed', t.getIndexes()[3].name);

    assert.eq({ a: -1, b: -1, c: 1 }, t.getIndexes()[4].key);
    assert.eq(db.getName() + '.loadvalidindexes', t.getIndexes()[4].ns);
    assert.eq('a_-1_b-1_c_1', t.getIndexes()[4].name);
    assert.eq(1024, t.getIndexes()[4].basement_size);
}();

// Test that the ns field gets inherited in each index spec object
// when it isn't provided.
var testIndexSpecNs = function() {
    t = db.loadindexspecns;
    t.drop();
    begin();
    beginLoad('loadindexspecns', [ { key: { a: 1 }, name: 'a_1' }, { key: { foo : -1 }, name: 'foobar' } ], { });
    commitLoad();
    commit();
    assert.eq(db.getName() + '.loadindexspecns', t.getIndexes()[0].ns);
    assert.eq(db.getName() + '.loadindexspecns', t.getIndexes()[1].ns);
    assert.eq(db.getName() + '.loadindexspecns', t.getIndexes()[2].ns);
}();

var testIndexedInsertCommit = function() {
    t = db.loaderindexedinsertcommit;
    t.drop();

    begin();
    beginLoad('loaderindexedinsertcommit', [ { key: { a: 1 }, name: "a_1" } ], { } );
    t.insert({ a: 700 } );
    commitLoad();
    commit();
    assert.eq(1, t.count({ a: 700 }));
    assert.eq(1, t.find({a : 700}).hint({ a: 1 }).itcount());
}();

var testUsableAfterCommit = function() {
    t = db.loaderusablecommit;
    t.drop();
    begin();
    beginLoad('loaderusablecommit', [ ], { });
    t.insert({ duringLoad: 1 });
    assert(!db.getLastError());
    commitLoad();
    commit();
    assert.eq(1, t.count());
    assert.eq(1, t.count({ duringLoad: 1 }));

    t.insert({ afterLoad: 1 });
    assert.eq(2, t.count());
    assert.eq(1, t.count({ duringLoad: 1 }));
    assert.eq(1, t.count({ afterLoad: 1 }));
    t.remove({ duringLoad: 1 });
    assert.eq(1, t.count());
}();

var testExternallyUsableAfterCommit = function() {
    t = db.loaderusablecommit1;
    t.drop();
    begin();
    beginLoad('loaderusablecommit1', [ ], { });
    t.insert({ duringLoad: 1 });
    assert(!db.getLastError());
    commitLoad();
    commit();
    assert.eq(1, t.count());
    assert.eq(1, t.count({ duringLoad: 1 }));

    s = startParallelShell('k = db.loaderusablecommit1.count(); print("second thread: count got " + k ); assert.eq(1, k); db.loaderusablecommit1.insert({ success: 1 }); assert(!db.getLastError());')
    s();
    assert.eq(1, t.count({ success: 1 }));
}();

var testUniquenessConstraint = function() {
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
    commitLoadShouldFail();
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
    commitLoadShouldFail();
    rollback();
    assert.eq(0, t.count());
}();
