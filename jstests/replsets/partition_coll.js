
// tests that cloning the oplog properly gives the same partitions

doTest = function (signal, startPort, txnLimit) {
    var num = 3;
    var host = getHostName();
    var name = "rollback_simple";
    var timeout = 60000;

    var replTest = new ReplSetTest( {name: name, nodes: num, startPort:startPort, txnMemLimit: txnLimit} );
    var conns = replTest.startSet();
    var port = replTest.ports;
    var config = {_id : name, members :
            [
             {_id:0, host : host+":"+port[0], priority:10 },
             {_id:1, host : host+":"+port[1]},
             {_id:2, host : host+":"+port[2], arbiterOnly : true},
            ],
             };

    replTest.initiate(config);
    replTest.awaitReplication();
    assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });

    conns[0].setSlaveOk();
    conns[1].setSlaveOk();

    // test creating a partitioned collection
    var a = conns[0].getDB("foo");
    var b = conns[1].getDB("foo");

    print("testing create\n");
    assert.commandWorked(a.runCommand({create:"foo", partitioned:1, primaryKey: {a:1, _id:1}}));
    replTest.awaitReplication();
    assert.commandWorked(b.foo.getPartitionInfo());
    x = a.foo.getPartitionInfo();
    y = b.foo.getPartitionInfo();
    assert(friendlyEqual(x,y));

    // now let's add a partition with a custom pivot
    print("testing add partition with custom pivot\n");
    assert.commandWorked(a.foo.addPartition({a:1, _id:10}));
    replTest.awaitReplication();
    assert.commandWorked(b.foo.getPartitionInfo());
    x = a.foo.getPartitionInfo();
    y = b.foo.getPartitionInfo();
    assert(friendlyEqual(x,y));

    // now add a partition without a custom pivot
    print("testing add partition without custom pivot\n");
    a.foo.insert({a:1,_id:1});
    a.foo.insert({a:2,_id:1});
    assert.commandWorked(a.foo.addPartition());
    replTest.awaitReplication();
    assert.commandWorked(b.foo.getPartitionInfo());
    x = a.foo.getPartitionInfo();
    y = b.foo.getPartitionInfo();
    assert(friendlyEqual(x,y));

    a.foo.insert({a:3,_id:3});
    replTest.awaitReplication();

    var ac = a.foo.find().sort({a:1}).toArray();
    var bc = b.foo.find().sort({a:1}).toArray();
    assert(ac.length == bc.length)
    for (var i = 0; i < ac.length; i++) {
        printjson(ac[i]);
        assert(friendlyEqual(ac[i], bc[i]));
    }
    assert(b.foo.count() == 3);

    print("testing drop partition\n");
    assert(x.numPartitions == 3);
    assert(x["partitions"][1]["_id"] == 1);
    assert.commandWorked(a.foo.dropPartition(1));
    replTest.awaitReplication();
    assert.commandWorked(b.foo.getPartitionInfo());
    x = a.foo.getPartitionInfo();
    y = b.foo.getPartitionInfo();
    assert(friendlyEqual(x,y));    

    var ac = a.foo.find().sort({a:1}).toArray();
    var bc = b.foo.find().sort({a:1}).toArray();
    assert(ac.length == bc.length)
    for (var i = 0; i < ac.length; i++) {
        printjson(ac[i]);
        assert(friendlyEqual(ac[i], bc[i]));
    }
    assert(b.foo.count() == 2);

    print("testing clone\n");
    assert.commandWorked(a.copyDatabase("foo", "bar"));
    replTest.awaitReplication();
    var d = conns[1].getDB("bar");
    var ac = a.foo.find().sort({a:1}).toArray();
    var dc = d.foo.find().sort({a:1}).toArray();
    assert(ac.length == dc.length)
    for (var i = 0; i < ac.length; i++) {
        assert(friendlyEqual(ac[i], dc[i]));
    }
    x = a.foo.getPartitionInfo();
    y = d.foo.getPartitionInfo();
    assert(friendlyEqual(x,y));    

    print("testing drop \n");
    a.foo.drop();
    replTest.awaitReplication();
    assert.commandFailed(a.foo.getPartitionInfo());
    
    print("partition_coll.js SUCCESS");
    replTest.stopSet(signal);
};


doTest(15, 41000, 1);
doTest(15, 31000, 1000000);

