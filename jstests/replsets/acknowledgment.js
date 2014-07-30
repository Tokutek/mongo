
// tests that cloning the oplog properly gives the same partitions

doTest = function (signal, startPort, txnLimit) {
    var num = 3;
    var host = getHostName();
    var name = "acknowledgement";
    var timeout = 60000;

    var replTest = new ReplSetTest( {name: name, nodes: num, startPort:startPort, txnMemLimit: txnLimit} );
    var conns = replTest.startSet();
    var port = replTest.ports;
    var config = {_id : name, members :
            [
             {_id:0, host : host+":"+port[0], priority:10 },
             {_id:1, host : host+":"+port[1]},
             {_id:2, host : host+":"+port[2]},
            ],
             };

    replTest.initiate(config);
    replTest.awaitReplication();
    assert.soon(function() {return conns[0].getDB("admin").isMaster().ismaster})

    var testdb = conns[0].getDB("test");
    // some dummy inserts
    testdb.foo.insert({_id:1});
    testdb.foo.insert({_id:2});
    testdb.foo.insert({_id:3});
    testdb.foo.insert({_id:4});
    replTest.awaitReplication();
    master = replTest.getMaster();
    testdb = master.getDB("test");
    // do a simple test of majority write concern working
    testdb.foo.insert({_id:5});
    var x = testdb.runCommand({ getLastError: 1, wtimeout : 10000, w : 3 });
    assert.eq(null, x.err);

    // now artificially increase the highestKnownPrimary in the GTIDManager
    print("artificially bumping up newPrimary, to make acknowledgement fail");
    assert.commandWorked(conns[1].getDB("admin").runCommand({_replSetGTIDHKP : 1, newPrimary : 100}));
    assert.commandWorked(conns[2].getDB("admin").runCommand({_replSetGTIDHKP : 1, newPrimary : 100}));
    testdb.foo.insert({_id:6});
    var x = testdb.runCommand({ getLastError: 1, wtimeout : 10000, w: 'majority' });
    assert.eq(x.err, "timeout");

    print("acknowledgement.js SUCCESS");
    replTest.stopSet(signal);
};


doTest(15, 31000, 1000000);

