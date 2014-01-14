
// tests that cloning the oplog properly gives the same partitions

doTest = function (signal, startPort, txnLimit) {

    var rt = new ReplSetTest( { name : "oplog_clone" , nodes: 1 , startPort:startPort, txnMemLimit: txnLimit} );

    var nodes = rt.startSet();
    rt.initiate();
    var master = rt.getMaster();

    var md = master.getDB( 'd' );
    var admindb = master.getDB("admin");
    var localdb = master.getDB("local");
    md.foo.insert({});
    md.getLastError();
    assert.commandWorked(admindb.runCommand({replAddPartition:1}));
    md.foo.insert({});
    md.getLastError();
    assert.commandWorked(admindb.runCommand({replAddPartition:1}));
    md.foo.insert({});
    md.getLastError();


    // add a secondary
    var slave = rt.add();
    rt.reInitiate();
    print ("initiation complete!");
    slave.setSlaveOk();

    // now check that oplog and oplog.refs is the same
    aop = localdb.runCommand({getPartitionInfo:"oplog.rs"});
    arefs = localdb.runCommand({getPartitionInfo:"oplog.refs"});
    assert.eq(aop.numPartitions, 3);
    if (txnLimit < 15) {
        assert.eq(arefs.numPartitions, 3);
    }
    else {
        assert.eq(arefs.numPartitions, 1);
    }
    bop = slave.getDB("local").runCommand({getPartitionInfo:"oplog.rs"});
    brefs = slave.getDB("local").runCommand({getPartitionInfo:"oplog.refs"});

    assert(friendlyEqual(aop, bop));
    assert(friendlyEqual(arefs, brefs));
    rt.stopSet(signal);
}
doTest(15, 41000, 1);
doTest(15, 31000, 1000000);

