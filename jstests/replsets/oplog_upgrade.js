// Test that adding partitions and trimming oplog works properly

var multiMachines = false;

restartOutOfReplset = function (replTest, i) {
    // stop secondary, bring it up outside of repl set
    // and add index
    print("shutting down");
    replTest.stop(i);
    print("restarting without replset");
    replTest.restartWithoutReplset(i);
}

restartInReplset = function(replTest, conns, i) {
    print("shutting down secondary");
    replTest.stop(i);
    print("restarting secondary in replset");
    replTest.restart(i);
    assert.soon(function() { return conns[i].getDB("admin").isMaster().ismaster || conns[i].getDB("admin").isMaster().secondary; });
}


doTest = function (signal, startPort, txnLimit) {
    var replTest;
    if (multiMachines) {
        replTest = new ReplSetTest({ name: 'unicomplex', nodes: 3, startPort:startPort, txnMemLimit: txnLimit});
    }
    else {
        replTest = new ReplSetTest({ name: 'unicomplex', nodes: 1, startPort:startPort, txnMemLimit: txnLimit});
    }
    var nodes = replTest.nodeList();

    var conns = replTest.startSet();
    if (multiMachines) {
        var r = replTest.initiate({ "_id": "unicomplex",
                                  "members": [
                                              { "_id": 0, "host": nodes[0] },
                                              { "_id": 1, "host": nodes[1] },
                                              { "_id": 2, "host": nodes[2], arbiterOnly: true}]
                              });
    }
    else {
        var r = replTest.initiate({ "_id": "unicomplex",
                                  "members": [
                                              { "_id": 0, "host": nodes[0], priority:10 }]
                                  });
    }

    var m = replTest.getMaster();
    m.getDB("foo").foo.insert({});
    m.getDB("foo").foo.insert({});
    m.getDB("foo").foo.insert({});
    m.getDB("foo").getLastError();
    pivot = m.getDB("local").oplog.rs.find().sort({_id:-1}).next()._id;
    print(pivot);
    refsPivot = pivot;
    if (txnLimit < 15) {
        refsPivot = conns[0].getDB("local").oplog.refs.find().sort({_id:-1}).next()._id;
    }
    replTest.awaitReplication();
    var beforeGTID = conns[0].getDB("local").oplog.rs.find().next()._id;

    restartOutOfReplset(replTest, 0);

    localdb = conns[0].getDB("local");
    assert.commandWorked(localdb.runCommand({create:'tmpOplog'}));
    assert.commandWorked(localdb.runCommand({create:'tmpOplogRef'}));
    assert.commandWorked(localdb.runCommand({create:'oplogControl'}));
    assert.commandWorked(localdb.runCommand({create:'oplogRefControl'}));
    var curs = localdb.oplog.rs.find();
    while (curs.hasNext()) {
        x = curs.next();
        localdb.tmpOplog.insert(x);
        localdb.oplogControl.insert(x);
    }
    curs = localdb.oplog.refs.find();
    while (curs.hasNext()) {
        x = curs.next();
        localdb.tmpOplogRef.insert(x);
        localdb.oplogRefControl.insert(x);
    }
    localdb.oplog.rs.drop();
    localdb.oplog.refs.drop();
    assert.commandWorked(localdb.tmpOplog.renameCollection("oplog.rs"));
    assert.commandWorked(localdb.tmpOplogRef.renameCollection("oplog.refs"));
    assert.commandFailed(localdb.runCommand({getPartitionInfo:"oplog.rs"}));
    assert.commandFailed(localdb.runCommand({getPartitionInfo:"oplog.refs"}));
    
    // at this point, we should have an oplog and oplog.refs that is not partitioned
    restartInReplset(replTest,conns, 0);
    localdb = conns[0].getDB("local");
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    assert(oplogPartitionInfo.numPartitions == 2);
    // verify the pivot
    print(pivot);
    print(oplogPartitionInfo["partitions"][0]["max"]["_id"]);
    assert(friendlyEqual(oplogPartitionInfo["partitions"][0]["max"]["_id"], pivot));
    x = localdb.oplog.rs.find();
    y = localdb.oplogControl.find();
    // make sure data is good
    while (x.hasNext()) {
        assert(y.hasNext());
        assert(friendlyEqual(x.next(),y.next()));
    }
    x = localdb.oplog.refs.find();
    y = localdb.oplogRefControl.find();
    // make sure data is good
    while (x.hasNext()) {
        a = x.next();
        printjson(a);
        assert(y.hasNext());
        b = y.next();
        printjson(b);
        assert(friendlyEqual(a,b));
    }
    assert(!y.hasNext());
    if (txnLimit < 15) {
        assert(refsPartitionInfo.numPartitions == 2);
        assert(friendlyEqual(refsPartitionInfo["partitions"][0]["max"]["_id"],refsPivot));
        // make sure maxRefGTID is set
        assert(friendlyEqual(refsPartitionInfo["partitions"][0]["maxRefGTID"], pivot));
    }
    else {
        assert(refsPartitionInfo.numPartitions == 1);
    }

    var master = replTest.getMaster();
    master.getDB("foo").foo.insert({});
    master.getDB("foo").getLastError();
    replTest.awaitReplication();
    insertedGTID = conns[0].getDB("local").oplog.rs.find().sort({_id:-1}).next()._id;

    // simple trim test
    assert.commandWorked(conns[0].getDB("admin").runCommand({replTrimOplog:1, ts:oplogPartitionInfo["partitions"][1]["createTime"]}));
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    assert(oplogPartitionInfo.numPartitions == 1);
    assert.eq(oplogPartitionInfo["partitions"][0]["_id"], 1);
    minGTID = conns[0].getDB("local").oplog.rs.find().next()._id;
    print("minGTID " + minGTID.printGTID());
    print("beforeGTID " + beforeGTID.printGTID());
    assert(minGTID.GTIDPri() > beforeGTID.GTIDPri() || minGTID.GTIDSec() > beforeGTID.GTIDSec());
    assert.eq(refsPartitionInfo.numPartitions, 1);
    print("oplog_upgrade SUCCESS");
    replTest.stopSet(signal);
}

multiMachines = true;
doTest(15, 41000, 1);
doTest(15, 31000, 1000000);
multiMachines = false;
doTest(15, 41000, 1);
doTest(15, 31000, 1000000);

