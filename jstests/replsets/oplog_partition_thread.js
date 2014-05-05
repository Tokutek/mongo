// Test that adding partitions and trimming oplog works properly

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

restartOutOfReplsetAsSecondary = function (replTest) {
    // stop secondary, bring it up outside of repl set
    // and add index
    print("shutting down");
    replTest.stop(1);
    print("restarting without replset");
    replTest.restartWithoutReplset(1);
}
restartInReplsetAsSecondary = function(replTest, conns) {
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary in replset");
    replTest.restart(1);
    assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });
}



doTest = function (signal, startPort, txnLimit, expireHours, expireDays, rewindTime, expectPartitionAdded) {
    var replTest = new ReplSetTest({ name: 'unicomplex', nodes: 1, startPort:startPort, txnMemLimit: txnLimit});
    var nodes = replTest.nodeList();

    var conns = replTest.startSet();
    var r = replTest.initiate({ "_id": "unicomplex",
                              "members": [
                                          { "_id": 0, "host": nodes[0], priority:10 }]
                              });

    // Make sure we have a master
    var master = replTest.getMaster();
    master.getDB("foo").foo.insert({});
    master.getDB("foo").getLastError();
    var localdb = master.getDB("local");
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    assert.eq(1, oplogPartitionInfo.numPartitions);
    assert.eq(1, refsPartitionInfo.numPartitions);
    assert.eq(undefined, refsPartitionInfo.maxRefGTID);

    restartOutOfReplset(replTest);

    // change create time to 1.5 hours in past
    var time = new Date();
    time.setTime(time.valueOf() - rewindTime); // 1.5 hours in the past
    conns[0].getDB("local").runCommand({_changePartitionCreateTime:"oplog.rs", index:0, createTime:time});

    restartInReplset(replTest, conns);
    // make sure that our test hook worked.
    oplogPartitionInfo = conns[0].getDB("local").runCommand({getPartitionInfo:"oplog.rs"});
    print(time);
    print(oplogPartitionInfo["partitions"][0]["createTime"]);

    // change the expireMillis
    assert.commandWorked(conns[0].getDB("admin").runCommand({replSetExpireOplog:1, expireOplogDays:expireDays, expireOplogHours:expireHours}));

    sleep(2000); // give the background thread a chance to do what it needs to do, add the partition
    assert.soon( function(){
        oplogPartitionInfo = conns[0].getDB("local").runCommand({getPartitionInfo:"oplog.rs"});
        refsPartitionInfo = conns[0].getDB("local").runCommand({getPartitionInfo:"oplog.refs"});
        if (expectPartitionAdded) {
            return (oplogPartitionInfo.numPartitions == 2);
        }
        else {
            return (oplogPartitionInfo.numPartitions == 1);
        }
    });

    oplogPartitionInfo = conns[0].getDB("local").runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = conns[0].getDB("local").runCommand({getPartitionInfo:"oplog.refs"});

    if (expectPartitionAdded) {
        assert.eq(2, oplogPartitionInfo.numPartitions);
        if (txnLimit < 15) {
            assert.eq(2, refsPartitionInfo.numPartitions);
        }
        else {
            assert.eq(1, refsPartitionInfo.numPartitions);
        }
    }
    else {
        assert.eq(1, oplogPartitionInfo.numPartitions);
        assert.eq(1, refsPartitionInfo.numPartitions);
    }

    replTest.stopSet(signal);
}


doOtherTest = function (signal, startPort, txnLimit, expireHours, expireDays, rewindTime, expectPartitionDropped) {
    var replTest = new ReplSetTest({ name: 'unicomplex', nodes: 1, startPort:startPort, txnMemLimit: txnLimit});
    var nodes = replTest.nodeList();

    var conns = replTest.startSet();
    var r = replTest.initiate({ "_id": "unicomplex",
                              "members": [
                                          { "_id": 0, "host": nodes[0], priority:10 }]
                              });

    // Make sure we have a master
    var master = replTest.getMaster();
    var localdb = master.getDB("local");
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    assert.eq(1, oplogPartitionInfo.numPartitions);
    assert.eq(1, refsPartitionInfo.numPartitions);
    assert.eq(undefined, refsPartitionInfo.maxRefGTID);

    // total of 3 partitions
    assert.commandWorked(master.getDB("admin").runCommand({replAddPartition:1}));
    master.getDB("foo").foo.insert({});
    master.getDB("foo").getLastError();
    assert.commandWorked(master.getDB("admin").runCommand({replAddPartition:1}));
    master.getDB("foo").foo.insert({});
    master.getDB("foo").getLastError();

    restartOutOfReplset(replTest);

    // change create time
    var time = new Date();
    print(time);
    time.setTime(time.valueOf() - rewindTime);
    conns[0].getDB("local").runCommand({_changePartitionCreateTime:"oplog.rs", index:0, createTime:time});
    conns[0].getDB("local").runCommand({_changePartitionCreateTime:"oplog.rs", index:1, createTime:time});

    restartInReplset(replTest, conns);
    // make sure that our test hook worked.
    oplogPartitionInfo = conns[0].getDB("local").runCommand({getPartitionInfo:"oplog.rs"});
    print(time);
    print(oplogPartitionInfo["partitions"][0]["createTime"]);

    // change the expireMillis
    assert.commandWorked(conns[0].getDB("admin").runCommand({replSetExpireOplog:1, expireOplogDays:expireDays, expireOplogHours:expireHours}));

    sleep(2000); // give the background thread a chance to do what it needs to do, add the partition
    assert.soon( function(){
        oplogPartitionInfo = conns[0].getDB("local").runCommand({getPartitionInfo:"oplog.rs"});
        if (expectPartitionDropped) {
            return (oplogPartitionInfo.numPartitions == 2);
        }
        else {
            return (oplogPartitionInfo.numPartitions == 3);
        }
    });


    oplogPartitionInfo = conns[0].getDB("local").runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = conns[0].getDB("local").runCommand({getPartitionInfo:"oplog.refs"});

    if (expectPartitionDropped) {
        assert.eq(2, oplogPartitionInfo.numPartitions);
        if (txnLimit < 15) {
            assert.eq(2, refsPartitionInfo.numPartitions);
        }
        else {
            assert.eq(1, refsPartitionInfo.numPartitions);
        }
    }
    else {
        assert.eq(3, oplogPartitionInfo.numPartitions);
        if (txnLimit < 15) {
            assert.eq(3, refsPartitionInfo.numPartitions);
        }
        else {
            assert.eq(1, refsPartitionInfo.numPartitions);
        }
    }

    replTest.stopSet(signal);
}

doThirdTest = function (signal, startPort, txnLimit) {
        replTest = new ReplSetTest({ name: 'unicomplex', nodes: 3, startPort:startPort, txnMemLimit: txnLimit});
    var nodes = replTest.nodeList();

    var conns = replTest.startSet();
        var r = replTest.initiate({ "_id": "unicomplex",
                                  "members": [
                                              { "_id": 0, "host": nodes[0], priority:10  },
                                              { "_id": 1, "host": nodes[1]},
                                              { "_id": 2, "host": nodes[2], arbiterOnly: true}]
                              });

    // Make sure we have a master
    var master = replTest.getMaster();
    var localdb = conns[1].getDB("local");
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    assert.eq(1, oplogPartitionInfo.numPartitions);
    assert.eq(1, refsPartitionInfo.numPartitions);
    assert.eq(undefined, refsPartitionInfo.maxRefGTID);

    // total of 3 partitions
    replTest.awaitReplication();
    assert.commandWorked(conns[1].getDB("admin").runCommand({replAddPartition:1}));
    master.getDB("foo").foo.insert({});
    master.getDB("foo").getLastError();
    replTest.awaitReplication();
    assert.commandWorked(conns[1].getDB("admin").runCommand({replAddPartition:1}));
    master.getDB("foo").foo.insert({});
    master.getDB("foo").getLastError();
    replTest.awaitReplication();
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    assert.eq(3, oplogPartitionInfo.numPartitions);
    assert.eq(0, oplogPartitionInfo["partitions"][0]["_id"]);
    assert.eq(1, oplogPartitionInfo["partitions"][1]["_id"]);
    assert.eq(2, oplogPartitionInfo["partitions"][2]["_id"]);
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    if (txnLimit < 15) {
        assert.eq(3, refsPartitionInfo.numPartitions);
        assert.eq(0, refsPartitionInfo["partitions"][0]["_id"]);
        assert.eq(1, refsPartitionInfo["partitions"][1]["_id"]);
        assert.eq(2, refsPartitionInfo["partitions"][2]["_id"]);
    }
    else {
        assert.eq(1, refsPartitionInfo.numPartitions);
        assert.eq(0, refsPartitionInfo["partitions"][0]["_id"]);
    }
    restartOutOfReplsetAsSecondary(replTest);
    // change create time
    var time = new Date();
    print(time);
    time.setTime(time.valueOf() - 3*60*60*1000); // rewind two hours
    // do it for all 3
    conns[1].getDB("local").runCommand({_changePartitionCreateTime:"oplog.rs", index:0, createTime:time});
    conns[1].getDB("local").runCommand({_changePartitionCreateTime:"oplog.rs", index:1, createTime:time});
    conns[1].getDB("local").runCommand({_changePartitionCreateTime:"oplog.rs", index:2, createTime:time});

    restartInReplsetAsSecondary(replTest, conns);
    // make sure that our test hook worked.
    oplogPartitionInfo = conns[1].getDB("local").runCommand({getPartitionInfo:"oplog.rs"});
    print(time);
    print(oplogPartitionInfo["partitions"][0]["createTime"]);

    // change the expireMillis
    assert.commandWorked(conns[1].getDB("admin").runCommand({replSetExpireOplog:1, expireOplogDays:0, expireOplogHours:1}));

    sleep(2000); // give the background thread a chance to do what it needs to do, add the partition
    assert.soon( function(){
        oplogPartitionInfo = conns[1].getDB("local").runCommand({getPartitionInfo:"oplog.rs"});
        printjson(oplogPartitionInfo);
        return (oplogPartitionInfo["partitions"][0]["_id"] == 2);
    });


    oplogPartitionInfo = conns[1].getDB("local").runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = conns[1].getDB("local").runCommand({getPartitionInfo:"oplog.refs"});

    assert.eq(2, oplogPartitionInfo.numPartitions);
    assert.eq(2, oplogPartitionInfo["partitions"][0]["_id"]);
    assert.eq(3, oplogPartitionInfo["partitions"][1]["_id"]);
    if (txnLimit < 15) {
        assert.eq(2, refsPartitionInfo.numPartitions);
        assert.eq(2, refsPartitionInfo["partitions"][0]["_id"]);
        assert.eq(3, refsPartitionInfo["partitions"][1]["_id"]);
    }
    else {
        assert.eq(1, refsPartitionInfo.numPartitions);
        assert.eq(0, refsPartitionInfo["partitions"][0]["_id"]);
    }

    replTest.stopSet(signal);
}

// test that add partition and drop partition can happen together
doThirdTest(15, 31000, 1000000);
doThirdTest(15, 41000, 1);

// tests for drop partition
doOtherTest(15, 41000, 1000000, 1, 0, 90*60000, true); // 1 hour, 90 minute rewind
doOtherTest(15, 41000, 1, 1, 0, 90*60000, true); // 1 hour, 90 minute rewind

doOtherTest(15, 41000, 1000000, 1, 0, 59*60000, false); // 1 hour expireOplog, 59 minutes rewind
doOtherTest(15, 41000, 1, 1, 0, 59*60000, false); // 1 hour expireOplog, 59 minutes rewind

doOtherTest(15, 31000, 1000000, 0, 0, 90*90*60000, false); // 0 expire oplog, 90 day long rewind
doOtherTest(15, 31000, 1, 0, 0, 90*90*60000, false); // 0 expire oplog, 90 day long rewind

doOtherTest(15, 31000, 1000000, 0, 1, 25*60*60000, true); // 1 day expireOplog, 25 hour rewind
doOtherTest(15, 31000, 1, 0, 1, 25*60*60000, true); // 1 day expireOplog, 25 hour rewind
doOtherTest(15, 31000, 1000000, 0, 1, 23*60*60000, false); // 1 day expireOplog, 23 hour rewind
doOtherTest(15, 31000, 1, 0, 1, 23*60*60000, false); // 1 day expireOplog, 23 hour rewind

// tests for add partition
doTest(15, 41000, 1000000, 23, 0, 90*60000, true); //23 hours
doTest(15, 41000, 1, 23, 0, 90*60000, true); //23 hours
doTest(15, 31000, 1000000, 0, 1, 90*60000, false); // 1 day
doTest(15, 31000, 1, 0, 1, 90*60000, false); // 1 day
doTest(15, 31000, 1000000, 0, 0, 90*60000, false); // 0
doTest(15, 31000, 1, 0, 0, 90*60000, false); // 0

doTest(15, 41000, 1000000, 23, 0, 25*60*60000, true); //23 hours
doTest(15, 41000, 1, 23, 0, 25*60*60000, true); //23 hours
doTest(15, 31000, 1000000, 0, 1, 25*60*60000, true); // 1 day
doTest(15, 31000, 1, 0, 1, 25*60*60000, true); // 1 day
doTest(15, 31000, 1000000, 0, 0, 25*60*60000, true); // 0
doTest(15, 31000, 1, 0, 0, 25*60*60000, true); // 0

