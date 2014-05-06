// Test that adding partitions and trimming oplog works properly


doOtherTest = function (signal, startPort, txnLimit) {
    var replTest = new ReplSetTest({ name: 'unicomplex', nodes: 1, startPort:startPort, txnMemLimit: txnLimit});
    var nodes = replTest.nodeList();

    var conns = replTest.startSet();
    var r = replTest.initiate({ "_id": "unicomplex",
                              "members": [
                                          { "_id": 0, "host": nodes[0], priority:10 }]
                              });

    // Make sure we have a master
    var master = replTest.getMaster();
    var a = master.getDB("foo");
    var localdb = master.getDB("local");
    var admindb = master.getDB("admin");
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    assert.eq(1, oplogPartitionInfo.numPartitions);
    assert.eq(1, refsPartitionInfo.numPartitions);

    // insert some rows
    a.foo.insert({a:0});
    a.foo.insert({a:0});
    a.foo.insert({a:0});
    assert.commandFailed(localdb.runCommand({addPartition:"oplog.rs"}));
    assert.commandWorked(localdb.runCommand({addPartition:"oplog.rs", force:1}));
    if (txnLimit < 15) {
        assert.commandFailed(localdb.runCommand({addPartition:"oplog.refs"}));
        assert.commandWorked(localdb.runCommand({addPartition:"oplog.refs", force:1}));
    }


    assert.commandFailed(localdb.runCommand({dropPartition:"oplog.rs", id:1}));
    assert.commandWorked(localdb.runCommand({dropPartition:"oplog.rs", id:1, force:1}));
    if (txnLimit < 15) {
        assert.commandFailed(localdb.runCommand({dropPartition:"oplog.refs", id:1}));
        assert.commandWorked(localdb.runCommand({dropPartition:"oplog.refs", id:1, force: 1}));
    }
    replTest.stopSet(signal);
}

doOtherTest( 15, 31000, 1);
doOtherTest( 15, 41000, 1000000);

