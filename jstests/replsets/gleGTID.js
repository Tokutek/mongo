
// tests that cloning the oplog properly gives the same partitions

doTest = function (signal, startPort, txnLimit) {
    var num = 3;
    var host = getHostName();
    var name = "gleGTID";
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
    assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });

    x = conns[2].getDB("admin").runCommand({_replSetHKP : 1, hkp : 10});
    assert.eq(x.ok, 1);
    assert.eq(x.ret, true);
    // show that a failover happened
    assert.soon(function() {var x = conns[2].getDB("admin").runCommand({replSetGetStatus:1}); return x["members"][2]["highestKnownPrimaryInReplSet"] > 10});
    assert.soon(function() {var x = conns[2].getDB("admin").runCommand({replSetGetStatus:1}); return x["members"][1]["highestKnownPrimaryInReplSet"] > 10});
    assert.soon(function() {var x = conns[2].getDB("admin").runCommand({replSetGetStatus:1}); return x["members"][0]["highestKnownPrimaryInReplSet"] > 10});


    //assert.soon( function() {
    var master = replTest.getMaster();
    var testdb = master.getDB("test");

    // some dummy inserts
    testdb.foo.insert({_id:1});
    var x = testdb.runCommand({ getLastError: 1, wtimeout : 10000, w : 3 });
    assert.eq(null, x.err);

    var lastGTID = master.getDB("local").oplog.rs.find().sort({$natural : -1}).next()._id;
    var pri = lastGTID.GTIDPri();
    print(lastGTID.printGTID());
    print(lastGTID.GTIDPri());
    assert(lastGTID.GTIDPri() > 10);
    //});

    // now do some gle tests
    var x = testdb.runCommand({ getLastError: 1, wtimeout : 5000, wgtid : GTID(pri, 0), w: 'majority' });
    assert.eq(x.err, null);
    var x = testdb.runCommand({ getLastError: 1, wtimeout : 5000, wgtid : GTID(pri, 100000), w: 'majority' });
    assert.eq(x.err, "timeout");
    var x = testdb.runCommand({ getLastError: 1, wtimeout : 5000, wgtid : GTID(pri-1, 100000), w: 'majority' });
    assert.eq(x.err, "failover");

    print("gleGTID.js SUCCESS");
    replTest.stopSet(signal);
};


doTest(15, 31000, 1000000);

