// Test a bulk load

var filename;
if (TestData.testDir !== undefined) {
    load(TestData.testDir + "../_loader_helpers.js");
} else {
    load('jstests/_loader_helpers.js');
}

var replTest = new ReplSetTest({ name: 'bulkload', nodes: 3 });
var nodes = replTest.nodeList();

var conns = replTest.startSet();
var r = replTest.initiate({ "_id": "bulkload",
                            "members": [
                                { "_id": 0, "host": nodes[0], priority:10 },
                                { "_id": 1, "host": nodes[1] },
                                { "_id": 2, "host": nodes[2], arbiterOnly: true}
                            ]});

// Make sure we have a master
var master = replTest.getMaster();
b_conn = conns[1];
b_conn.setSlaveOk();

var masterdb = master.getDB('db');
var slavedb = b_conn.getDB('db');

var runTest = function(doCommit) {
    masterdb.replset_bulkload.drop();
    assert.eq(null, masterdb.getLastError());
    assert.commandWorked(masterdb.beginTransaction('serializable'));
    assert.commandWorked(masterdb.runCommand({
        beginLoad: 1,
        ns: 'replset_bulkload',
        indexes: [ { key: { a: 1 }, name: "a_1" } ],
        options: { compression: 'quicklz' }
    }));
    masterdb.replset_bulkload.insert({ _id: 0, a: 1, b: 1 });
    masterdb.replset_bulkload.insert({ _id: 1, a: 2, b: 2 });
    if (doCommit) {
        assert.commandWorked(masterdb.runCommand({ commitLoad: 1 }));
        assert.commandWorked(masterdb.commitTransaction());
    } else {
        assert.commandWorked(masterdb.runCommand({ abortLoad: 1 }));
        assert.commandWorked(masterdb.rollbackTransaction());
    }
    replTest.awaitReplication();
    if (doCommit) {
        // Two documents loaded
        assert.eq(2, masterdb.replset_bulkload.count());
        assert.eq(2, slavedb.replset_bulkload.count());
        // Three entries in system.namespaces containing *replset_bulkload*
        // - for the collection, for the _id index, for the 'a' index
        assert.eq(3, masterdb.system.namespaces.count({ name: /replset_bulkload/ }))
        assert.eq(3, slavedb.system.namespaces.count({ name: /replset_bulkload/ }))
        // Two indexes, _id and { a: 1 }
        assert.eq(2, masterdb.replset_bulkload.stats().nindexes);
        assert.eq(2, slavedb.replset_bulkload.stats().nindexes);
        assert.eq('quicklz', masterdb.replset_bulkload.stats().indexDetails[0].compression);

        // Writable?
        masterdb.replset_bulkload.insert({ _id: 3, a: 3 });
        assert.eq(null, masterdb.getLastError());
        // Readable?
        replTest.awaitReplication();
        assert.eq(3, masterdb.replset_bulkload.count());
        assert.eq(3, slavedb.replset_bulkload.count());
    } else {
        assert.eq(0, masterdb.replset_bulkload.count());
        assert.eq(0, slavedb.replset_bulkload.count());
        assert.eq(0, masterdb.system.namespaces.count({ name: /replset_bulkload/ }))
        assert.eq(0, slavedb.system.namespaces.count({ name: /replset_bulkload/ }))

        // Writable?
        masterdb.replset_bulkload.insert({ _id: 3, a: 3 });
        assert.eq(null, masterdb.getLastError());
        replTest.awaitReplication();
        // Readable?
        assert.eq(1, masterdb.replset_bulkload.count());
        assert.eq(1, slavedb.replset_bulkload.count());
    }
}

runTest(true);
runTest(false);
replTest.stopSet();
