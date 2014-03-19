// Test that adding partitions and trimming oplog works properly

var pivot;
var pivot2;
var pivot3;
var oplogPartitionInfo;
var refsPartitionInfo;


verifyStuff = function (txnLimit, num) {
    assert.eq(num, oplogPartitionInfo.numPartitions);
    if (num - 4 >= 0) {
        assert(friendlyEqual(pivot, oplogPartitionInfo["partitions"][num-4]["max"]["_id"]));
    }
    if (num - 3 >= 0) {
        assert(friendlyEqual(pivot2, oplogPartitionInfo["partitions"][num-3]["max"]["_id"]));
    }
    if (num - 2 >= 1) {
        assert(friendlyEqual(pivot3, oplogPartitionInfo["partitions"][num-2]["max"][_id""]));
    }
    if (txnLimit < 10) {
        assert.eq(num, refsPartitionInfo.numPartitions);
        if (num - 4 >= 0) {
        assert(friendlyEqual(pivot, refsPartitionInfo["partitions"][num-4]["maxRefGTID"]))
        }
        if (num - 3 >= 0) {
        assert(friendlyEqual(pivot2, refsPartitionInfo["partitions"][num-3]["maxRefGTID"]))
        }
        if (num - 2 >= 1) {
        assert(friendlyEqual(pivot3, refsPartitionInfo["partitions"][num-2]["maxRefGTID"]))
        }
        assert.eq(undefined, refsPartitionInfo["partitions"][num-1]["maxRefGTID"]);
    }
    else {
        assert.eq(1, refsPartitionInfo.numPartitions);
    }
}

doOtherTest = function (signal, startPort, txnLimit, trimWithGTID) {
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
    assert.eq(undefined, refsPartitionInfo.maxRefGTID);

    // insert some rows
    a.foo.insert({a:0});
    a.foo.insert({a:0});
    a.foo.insert({a:0});

    // this value should be come the pivot
    pivot = localdb.oplog.rs.find().sort({_id:-1}).next()._id;
    assert.commandWorked(admindb.runCommand({replAddPartition:1}));
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    assert.eq(2, oplogPartitionInfo.numPartitions);
    assert(friendlyEqual(pivot, oplogPartitionInfo["partitions"][0]["max"]["_id"]));
    if (txnLimit < 10) {
        assert.eq(2, refsPartitionInfo.numPartitions);
        assert(friendlyEqual(pivot, refsPartitionInfo["partitions"][0]["maxRefGTID"]));
        assert.eq(undefined, refsPartitionInfo["partitions"][1]["maxRefGTID"]);
    }
    else {
        assert.eq(1, refsPartitionInfo.numPartitions);
    }

    // get first entry
    var c = localdb.oplog.rs.find();

    // insert some rows
    a.foo.insert({a:0});
    a.foo.insert({a:0});
    a.foo.insert({a:0});
    pivot2 = localdb.oplog.rs.find().sort({_id:-1}).next()._id;
    assert.commandFailed(localdb.runCommand({addPartition:"oplog.rs"}));
    if (txnLimit < 15) {
        assert.commandFailed(localdb.runCommand({addPartition:"oplog.refs"}));
    }
    assert.commandWorked(admindb.runCommand({replAddPartition:1}));


    // insert some rows
    a.foo.insert({a:0});
    a.foo.insert({a:0});
    a.foo.insert({a:0});
    pivot3 = localdb.oplog.rs.find().sort({_id:-1}).next()._id;
    assert.commandWorked(admindb.runCommand({replAddPartition:1}));
    // add data after this add partition, so we can find something
    // in search further down
    a.foo.insert({a:0});

    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    verifyStuff(txnLimit, 4);
    
    // now let's test test trim

    // first test that a trim with parameters too low will not trim anything
    assert.commandFailed(localdb.runCommand({dropPartition:"oplog.rs", id:0}));
    assert.commandFailed(localdb.runCommand({dropPartition:"oplog.refs", id:0}));
    assert.commandWorked(admindb.runCommand({replTrimOplog:1, ts:ISODate("1981-11-01T17:17:44.477Z")}));
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    verifyStuff(txnLimit, 4);

    // gtid maps to (1,0)
    assert.commandWorked(admindb.runCommand({replTrimOplog:1, gtid:BinData(0,"AAAAAAAAAAEAAAAAAAAAAA==")}));
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    verifyStuff(txnLimit, 4);

    // now do a boundary case for timestamp
    assert.commandWorked(admindb.runCommand({replTrimOplog:1, ts:oplogPartitionInfo["partitions"][1]["createTime"]}));
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    // one partition should have dropped
    verifyStuff(txnLimit, 3);

    var time = new Date();
    time.setTime(time.valueOf() + 150*60000); // 2.5 hours in the future
    assert.commandFailed(admindb.runCommand({replTrimOplog:1}));
    assert.commandFailed(admindb.runCommand({replTrimOplog:1, gtid:pivot3, ts:time}));
    // test that trimming with a future timestamp leaves one partition
    if (trimWithGTID == 0) {
        assert.commandWorked(admindb.runCommand({replTrimOplog:1, ts:time}));
    }
    else {
        assert.commandWorked(admindb.runCommand({replTrimOplog:1, gtid:pivot3}));
    }
    var newOplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    var newRefsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    assert.eq(newOplogPartitionInfo.numPartitions, 1);
    assert.eq(newRefsPartitionInfo.numPartitions, 1);
    // check the partition info is the same
    assert.eq(newOplogPartitionInfo["partitions"][0]._id, oplogPartitionInfo["partitions"][2]._id);
    assert.eq(newOplogPartitionInfo["partitions"][0]["max"], oplogPartitionInfo["partitions"][2]["max"]);
    // check oplog data
    x = localdb.oplog.rs.find().next()._id;
    assert(x > pivot3);

    replTest.stopSet(signal);
}

// this test verifies that if we have a spilled transaction
// that goes over several partitions in oplog.refs, that
// the maxRefGTID is properly set in the proper partitions
// and the data does not get prematurely freed

doTest = function (signal, startPort) {
    var replTest = new ReplSetTest({ name: 'unicomplex', nodes: 1, startPort:startPort, txnMemLimit: 1});
    var nodes = replTest.nodeList();

    var conns = replTest.startSet();
    var r = replTest.initiate({ "_id": "unicomplex",
                              "members": [
                                          { "_id": 0, "host": nodes[0], priority:10 }]
                              });



    var bigTxnConn = new Mongo(replTest.getURL());
    var addPartConn = new Mongo(replTest.getURL());
    var checkConn = new Mongo(replTest.getURL());
    print ("conns created\n");
    var localdb = checkConn.getDB("local");
    print ("1\n");

    assert.commandWorked(bigTxnConn.getDB("foo").beginTransaction());
    print ("2\n");

    bigTxnConn.getDB("foo").foo.insert({});
    bigTxnConn.getDB("foo").getLastError();
    addPartConn.getDB("bar").bar.insert({});
    addPartConn.getDB("foo").getLastError();
    pivot = localdb.oplog.rs.find().sort({_id:-1}).next()._id;
    assert.commandWorked(addPartConn.getDB("admin").runCommand({replAddPartition:1}));

    bigTxnConn.getDB("foo").foo.insert({});
    bigTxnConn.getDB("foo").getLastError();
    addPartConn.getDB("bar").bar.insert({});
    addPartConn.getDB("foo").getLastError();
    pivot2 = localdb.oplog.rs.find().sort({_id:-1}).next()._id;
    assert.commandWorked(addPartConn.getDB("admin").runCommand({replAddPartition:1}));

    bigTxnConn.getDB("foo").foo.insert({});
    bigTxnConn.getDB("foo").getLastError();
    addPartConn.getDB("bar").bar.insert({});
    addPartConn.getDB("foo").getLastError();
    pivot3 = localdb.oplog.rs.find().sort({_id:-1}).next()._id;
    assert.commandWorked(addPartConn.getDB("admin").runCommand({replAddPartition:1}));

    bigTxnConn.getDB("foo").foo.insert({});
    bigTxnConn.getDB("foo").getLastError();
    print ("inserts made\n");

    // at this point, we should have 4 partitions in both the oplog and oplog.refs
    // and the maxRefGTID should match the pivots
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    verifyStuff(1,4);
    print ("verify done\n");
    printjson(refsPartitionInfo["partitions"][0]["maxRefGTID"]);

    assert.commandWorked(bigTxnConn.getDB("foo").commitTransaction());
    bigTxnConn.getDB("foo").getLastError()

    print("********************Contents of refs************************");
    x = localdb.oplog.refs.find();
    while (x.hasNext()) {
        printjson(x.next());
    }
    print("************************************************************");

    // now we ought to verify that the maxRefGTID of all of the partitions is
    // the last GTID in the oplog
    var lastGTID = localdb.oplog.rs.find().sort({_id:-1}).next()._id;
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    printjson(oplogPartitionInfo);
    printjson(refsPartitionInfo);
    printjson(lastGTID);
    printjson(refsPartitionInfo["partitions"][0]["maxRefGTID"]);
    assert.eq(undefined, refsPartitionInfo["partitions"][3]["maxRefGTID"]);
    assert(friendlyEqual(lastGTID, refsPartitionInfo["partitions"][2]["maxRefGTID"]));
    assert(friendlyEqual(lastGTID, refsPartitionInfo["partitions"][1]["maxRefGTID"]));
    assert(friendlyEqual(lastGTID, refsPartitionInfo["partitions"][0]["maxRefGTID"]));

    // now drop all partitions but the last one
    assert.commandWorked(addPartConn.getDB("admin").runCommand({replTrimOplog:1, gtid:pivot3}));
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    assert.eq(oplogPartitionInfo.numPartitions, 1);
    assert.eq(refsPartitionInfo.numPartitions, 4); // none of these should have dropped

    // add another partition
    assert.commandWorked(addPartConn.getDB("admin").runCommand({replAddPartition:1}));
    var time = new Date();
    time.setTime(time.valueOf() + 150*60000); // 2.5 hours in the future
    assert.commandWorked(addPartConn.getDB("admin").runCommand({replTrimOplog:1, ts:time}));
    // now there should be just one partition for each
    oplogPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    refsPartitionInfo = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    assert.eq(oplogPartitionInfo.numPartitions, 1);
    assert.eq(refsPartitionInfo.numPartitions, 1);

    replTest.stopSet(signal);
}

doTest( 15, 31000 );
doOtherTest( 15, 31000, 1, 0 );
doOtherTest( 15, 31000, 1, 1 );
doOtherTest( 15, 41000, 1000000, 0 );
doOtherTest( 15, 41000, 1000000, 1 );

