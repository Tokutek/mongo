// Test that the loader behaves well with the multi-statement transaction API. 

var filename;
if (TestData.testDir !== undefined) {
    load(TestData.testDir + "/_loader_helpers.js");
} else {
    load('jstests/_loader_helpers.js');
}

var testNoTransaction = function() {
    t = db.loadnotxn;
    t.drop();
    beginLoadShouldFail('loadnotxn', [ ] , { });
}();

var testEarlyCommit = function() {
    t = db.loadearlycommit;
    t.drop();
    begin();
    beginLoad('loadearlycommit', [ ], { });
    assert.throws(db.runCommand({ 'commitTransaction' : 1 }));
    commitLoad();
    commit();

    t.drop();
    begin();
    beginLoad('loadearlycommit', [ ], { });
    assert.throws(db.runCommand({ 'commitTransaction' : 1 }));
    abortLoad();
    commit();
}();

var testEarlyRollback = function() {
    t = db.loaderearlyrollback;
    t.drop();
    begin();
    beginLoad('loaderearlyrollback', [ ], { });
    assert.throws(db.runCommand({ 'rollbackTransaction' : 1 }));
    abortLoad();
    rollback();

    t.drop();
    begin();
    beginLoad('loaderearlyrollback', [ ], { });
    assert.throws(db.runCommand({ 'rollbackTransaction' : 1 }));
    commitLoad();
    rollback();
}();

var testMultiStatementCommit = function() {
    t = db.loadermstcommit;
    t2 = db.loadermstcommit2;
    t.drop();
    t2.drop();
    begin();
    t.insert({});
    assert.eq(1, t.count());
    beginLoad('loadermstcommit2', [ ], { });
    commitLoad();
    assert.eq(1, t.count());
    t.insert({});
    assert.eq(2, t.count());
    commit();
    assert.eq(2, t.count());

    // Test rollback
    t.drop();
    t2.drop();
    begin();
    t.insert({});
    assert.eq(1, t.count());
    beginLoad('loadermstcommit2', [ ], { });
    commitLoad();
    assert.eq(1, t.count());
    t.insert({});
    assert.eq(2, t.count());
    rollback();
    assert.eq(0, t.count());
}();

var testMultiStatementAbort = function() {
    t = db.loadermstabort;
    t2 = db.loadermstabort2;
    t.drop();
    t2.drop();
    begin();
    t.insert({});
    assert.eq(1, t.count());
    beginLoad('loadermstabort2', [ ], { });
    abortLoad();
    assert.eq(1, t.count());
    t.insert({});
    assert.eq(2, t.count());
    commit();
    assert.eq(2, t.count());

    // Test rollback
    t.drop();
    t2.drop();
    begin();
    t.insert({});
    assert.eq(1, t.count());
    beginLoad('loadermstabort2', [ ], { });
    abortLoad();
    assert.eq(1, t.count());
    t.insert({});
    assert.eq(2, t.count());
    rollback();
    assert.eq(0, t.count());
}();

var testUnrelatedOpsDuringLoad = function() {
    t = db.loaderunrelated;
    t2 = db.loaderunrelated2;
    t.drop();
    t2.drop();
    begin();
    beginLoad('loaderunrelated2', [ ], { });
    t.insert({});
    assert.eq(1, t.count());
    commitLoad();
    assert.eq(1, t.count());
    t.insert({});
    assert.eq(2, t.count());
    commit();
    assert.eq(2, t.count());

    // Test rollback
    t.drop();
    t2.drop();
    begin();
    beginLoad('loaderunrelated2', [ ], { });
    t.insert({});
    assert.eq(1, t.count());
    abortLoad();
    assert.eq(0, t.count());
    t.insert({});
    assert.eq(1, t.count());
    rollback();
    assert.eq(0, t.count());
}();
