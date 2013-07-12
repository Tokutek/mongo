// Test that the beginLoadShouldFail interface validates parameters
// and enforces the rules of beginning a bulk load.

load("jstests/loader_helpers.js");

var testNSAlreadyExists = function() {
    t = db.loadnsexists;
    t.drop();
    t.insert({});
    assert(!db.getLastError());
    assert.eq(1, db.system.namespaces.count({ "name" : "test.loadnsexists" }));

    begin();
    beginLoadShouldFail('loadnsexists', [ ] , { });
    commit();
    assert.eq(1, db.system.namespaces.count({ "name" : "test.loadnsexists" }));
}();

var testNSProvisionallyExists = function() {
    t = db.loadnsprovexists;
    t.drop();
    begin();
    s = startParallelShell('db.runCommand({ beginTransaction: 1 });' +
                           'db.loadnsprovexists.insert({ prov: 1 });' +
                           'sleep(2000); db.runCommand({ commitTransaction: 1 })');
    sleep(500);
    beginLoadShouldFail('loadnsprovexists', [ ], { });
    commit();
    s();
}();

var testNSProvisionallyDropped = function() {
    t = db.loadnsprovdropped;
    t.drop();
    t.insert({ prov: 1 });
    begin();
    s = startParallelShell('db.runCommand({ beginTransaction: 1 });' +
                           'db.loadnsprovdropped.drop(); ' +
                           'sleep(2000); db.runCommand({ commitTransaction: 1 })');
    sleep(500);
    beginLoadShouldFail('loadnsprovdropped', [ ], { });
    commit();
    s();
}();

var testBadIndexes = function() {
    t = db.loadbadindexes;
    t.drop();

    begin();
    beginLoadShouldFail('loadbadindexes', "xyz" , { });
    commit();
    assert.eq(0, db.system.namespaces.count({ "name" : "test.loadbadindexes" }));

    begin();
    beginLoadShouldFail('loadbadindexes', [ { ns: "test.loadbadindexes", key: { a: 1 }, name: "a_1" }, "xyz" ] , { });
    commit();
    assert.eq(0, db.system.namespaces.count({ "name" : "test.loadbadindexes" }));

    begin();
    beginLoadShouldFail('loadbadindexes', [ "xyz", { ns: "test.loadbadindexes", key: { a: 1 }, name: "a_1" } ] , { });
    commit();
    assert.eq(0, db.system.namespaces.count({ "name" : "test.loadbadindexes" }));

    begin();
    beginLoadShouldFail('loadbadindexes', [ { ns: "test.thisisntright", key: { a: 1 }, name: "a_1" } ] , { });
    commit();
    assert.eq(0, db.system.namespaces.count({ "name" : "test.loadbadindexes" }));
    assert.eq(0, db.system.namespaces.count({ "name" : "test.thisisntright" }));

    begin();
    beginLoadShouldFail('loadbadindexes', [ { ns: "test.loadbadindexes", key: "xyz", name: "xyz_1" } ] , { });
    commit();
    assert.eq(0, db.system.namespaces.count({ "name" : "test.loadbadindexes" }));

    begin();
    beginLoadShouldFail('loadbadindexes', [ { ns: "test.loadbadindexes", key: { a: 1 }, name: { thisisntright: 1 } } ] , { });
    commit();
    assert.eq(0, db.system.namespaces.count({ "name" : "test.loadbadindexes" }));
}();

var testBadOptions = function() {
    t = db.loadbadoptions;
    t.drop();

    begin();
    beginLoadShouldFail('loadbadoptions', [ ] , "xyz");
    commit();
    assert.eq(0, db.system.namespaces.count({ "name" : "test.loadbadoptions" }));

    begin();
    beginLoadShouldFail('loadbadoptions', [ ] , 123);
    commit();
    assert.eq(0, db.system.namespaces.count({ "name" : "test.loadbadoptions" }));

    begin();
    beginLoadShouldFail('loadbadoptions', [ ] , [ { clustering: true } ]);
    commit();
    assert.eq(0, db.system.namespaces.count({ "name" : "test.loadbadoptions" }));
}();
