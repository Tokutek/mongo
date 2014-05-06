
// test oplog versioning works (#780)

doTest = function (signal, startPort, txnLimit) {

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
    replTest.bridge();

    assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });
    var master = conns[0];
    var slave = conns[1];
    slave.setSlaveOk();
    var mfoo = master.getDB("foo");
    var sfoo = slave.getDB("foo");
    mfoo.foo.insert({_id:1});
    replTest.awaitReplication();

    // now we disconnect 0 from 1
    print("disconnecting 0 and 1");
    replTest.partition(0,1);
    print("changing oplog version");
    assert.commandWorked(sfoo.runCommand({_changeOplogVersion:1}));
    print("changed oplog version, inserting 2");
    mfoo.foo.insert({_id:2});
    print("connecting them back");
    replTest.unPartition(0,1);
    print("waiting to become secondary");
    assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });
    print("am secondary, waiting 10 seconds now");
    sleep(10000);
    assert.eq(mfoo.foo.count(), 2);
    assert.eq(sfoo.foo.count(), 1); // should not replicate
    
    replTest.stopSet(signal);
}
doTest(15, 31000, 1000000);

