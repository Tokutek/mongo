
// tests that cloning the oplog properly gives the same partitions

restartOutOfReplset = function (replTest) {
    // stop secondary, bring it up outside of repl set
    // and add index
    print("shutting down");
    replTest.stop(0);
    print("restarting without replset");
    replTest.restartWithoutReplset(0);
}

restartInReplset = function(replTest, conns) {
    print("shutting down secondary");
    replTest.stop(0);
    print("restarting secondary in replset");
    replTest.restart(0);
    assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });
}


doTest = function (signal, startPort, txnLimit) {

    var replTest = new ReplSetTest({ name: 'unicomplex', nodes: 1, startPort:startPort, txnMemLimit: txnLimit});
    var nodes = replTest.nodeList();

    var conns = replTest.startSet();
    var r = replTest.initiate({ "_id": "unicomplex",
                              "members": [
                                          { "_id": 0, "host": nodes[0], priority:10 }]
                              });

    // Make sure we have a master
    var master = replTest.getMaster();
    restartOutOfReplset(replTest);
    tdb = conns[0].getDB("test");
    assert.commandWorked(tdb.runCommand({create:"part", partitioned:1}));
    assert.commandWorked(tdb.runCommand({addPartition:"part", newPivot:{_id:10}}));
    assert.commandWorked(tdb.runCommand({addPartition:"part", newPivot:{_id:20}}));
    restartInReplset(replTest,conns);
    tdb = conns[0].getDB("test");
    print("************************ruh roh*******************************\n");
    assert.commandFailed(tdb.runCommand({addPartition:"part", newPivot:{_id:100}}));
    print("************************ruh roh1*******************************\n");
    assert.commandFailed(tdb.runCommand({dropPartition:"part", id:0}));
    print("************************ruh roh2*******************************\n");
    assert.commandFailed(conns[0].getDB("admin").runCommand({copydb:1, fromhost: "localhost:" + startPort, fromdb: "test", todb: "abra"}));

}
doTest(15, 41000, 1);
doTest(15, 31000, 1000000);

