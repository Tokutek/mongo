
// test that initial sync works when a profile collection exists on master
// and that the profile collection does not get created on slave

doTest = function (signal, startPort, txnLimit) {

    var rt = new ReplSetTest( { name : "oplog_clone" , nodes: 1 , startPort:startPort, txnMemLimit: txnLimit} );

    var nodes = rt.startSet();
    rt.initiate();
    var master = rt.getMaster();
    master.getDB("foo").createCollection("foo", {capped:1, size:11111});
    master.getDB("foo").setProfilingLevel(2); // make profiling collection


    // add a secondary
    var slave = rt.add();
    rt.reInitiate();
    print ("initiation complete!");
    slave.setSlaveOk();

    rt.awaitReplication();
    // now verify that master has a profiling collection and slave does not
    y = master.getDB("foo").system.namespaces.find();
    while(y.hasNext()) {
        printjson(y.next());
    }
    x = master.getDB("foo").system.namespaces.count({name:"foo.system.profile"});
    assert.eq(x,1);
    x = slave.getDB("foo").system.namespaces.count({name:"foo.system.profile"});
    assert.eq(x,0);

    rt.stopSet(signal);
}
doTest(15, 31000, 1000000);

