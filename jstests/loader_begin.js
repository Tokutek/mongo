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
    s = startParallelShell('db.beginTransaction();' +
                           'db.loadnsprovexists.insert({ prov: 1 });' +
                           'sleep(2000); db.commitTransaction()');
    sleep(1000);
    beginLoadShouldFail('loadnsprovexists', [ ], { });
    commit();
    s();
}();

var testNSProvisionallyDropped = function() {
    t = db.loadnsprovdropped;
    t.drop();
    t.insert({ prov: 1 });
    begin();
    s = startParallelShell('db.beginTransaction();' +
                           'db.loadnsprovdropped.drop(); ' +
                           'sleep(2000); db.rollbackTransaction()');
    sleep(500);
    beginLoadShouldFail('loadnsprovdropped', [ ], { });
    commit();
    assert.eq(1, t.count({ prov : 1 }));
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

var testCappedLoadFails = function() {
    t = db.loadcappedfails;
    t.drop();

    // Test explicitly specifying the $_ index
    begin();
    beginLoadShouldFail('loadcappedfails', [ { ns: "test.loadcappedfails", key: { "$_" : 1 }, name: "$_1" } ], { capped: true, size: 1024 } );
    commit();
    assert.eq(0, db.system.namespaces.find({ "name": "test.loadcappedfails" }).itcount());

    // Test without explicitly specifying the $_ index
    begin();
    beginLoadShouldFail('loadcappedfails', [ { ns: "test.loadcappedfails", key: { "a" : 1 }, name: "$_1" } ], { capped: true, size: 1024 } );
    commit();
    assert.eq(0, db.system.namespaces.find({ "name": "test.loadcappedfails" }).itcount());
}();

var testNaturalOrderLoadFails = function() {
    t = db.loadnaturalfails;
    t.drop();

    begin();
    beginLoadShouldFail('loadnaturalfails', [ { ns: "test.loadnaturalfails", key: { "a" : 1 }, name: "$_1" } ],  { natural: 1 } );
    commit();
    assert.eq(0, db.system.namespaces.find({ "name": "test.loadnaturalfails" }).itcount());

    begin();
    beginLoadShouldFail('loadnaturalfails', [ { ns: "test.loadnaturalfails", key: { "$_" : 1 }, name: "$_1" }, { ns: "test.loadnaturalfails", key: { "a" : 1 }, name: "a_1" } ],  { natural: 1 } );
    commit();
    assert.eq(0, db.system.namespaces.find({ "name": "test.loadnaturalfails" }).itcount());
}();

var testSystemCatalogOrProfileLoadFails = function() {
    db.dropDatabase(); // so no catalog collections exist
    begin();
    beginLoadShouldFail('system.indexes', [ ], { });
    beginLoadShouldFail('system.indexes', [ { ns: 'test.system.indexes', key: { "$_" : 1 } , name: "$_1" } ], { });
    beginLoadShouldFail('system.namespaces', [ ], { });
    beginLoadShouldFail('system.namespaces', [ { ns: 'test.system.namespaces', key: { "$_" : 1 } , name: "$_1" } ], { });
    beginLoadShouldFail('system.profile', [ ], { });
    beginLoadShouldFail('system.profile', [ { ns: 'test.system.profile', key: { "$_" : 1 } , name: "$_1" } ], { });
    beginLoadShouldFail('system.profile', [ { ns: 'test.system.profile', key: { "$_" : 1 } , name: "$_1" } ], { capped: true, size: 1024 });
    commit();
}();

var testBadPrimaryKeyOptions = function() {
    t = db.loadbadpk;
    t.drop();

    begin();
    beginLoadShouldFail('loadbadpk', [ { ns: "test.loadbadpk", key: { b: 1 }, name: "b_1" } ], { primaryKey: { a: 1 } }); 
    commit();
    assert.eq(0, db.system.namespaces.find({ "name": "test.loadbadpk" }).itcount());

    begin();
    beginLoadShouldFail('loadbadpk', [ { ns: "test.loadbadpk", key: { b: 1 }, name: "b_1" } ], { primaryKey: { _id: 1, a: 1 } }); 
    commit();
    assert.eq(0, db.system.namespaces.find({ "name": "test.loadbadpk" }).itcount());
}();
