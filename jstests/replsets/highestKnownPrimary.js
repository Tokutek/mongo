
// tests that cloning the oplog properly gives the same partitions

doTest = function (signal, startPort, txnLimit) {
    var num = 3;
    var host = getHostName();
    var name = "highestKnownPrimary";
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

    x = conns[2].getDB("admin").runCommand({_replSetHKP : 1, hkp : 10});
    assert.eq(x.ok, 1);
    assert.eq(x.ret, true);
    assert.soon(function() {var x = conns[2].getDB("admin").runCommand({replSetGetStatus:1}); return x["members"][2]["highestKnownPrimaryInReplSet"] > 10});
    assert.soon(function() {var x = conns[2].getDB("admin").runCommand({replSetGetStatus:1}); return x["members"][1]["highestKnownPrimaryInReplSet"] > 10});
    assert.soon(function() {var x = conns[2].getDB("admin").runCommand({replSetGetStatus:1}); return x["members"][0]["highestKnownPrimaryInReplSet"] > 10});

    var master = replTest.getMaster();
    var lastGTID = master.getDB("local").oplog.rs.find().sort({$natural : -1}).next()._id;
    print(lastGTID.printGTID());
    print(lastGTID.GTIDPri());
    assert(lastGTID.GTIDPri() > 10);

    // make sure vote info gets properly written to replVote
    x = conns[2].getDB("local").replVote.find().next();
    assert.eq(x._id, "highestVote");
    assert(x.val > 10);
    var newVal = x.val;

    x = conns[1].getDB("local").replVote.find().next();
    assert.eq(x._id, "highestVote");
    assert.eq(x.val, newVal);

    x = master.getDB("local").replVote.find().next();
    assert.eq(x._id, "highestVote");
    assert.eq(x.val, newVal);

    x = conns[2].getDB("admin").runCommand({_replSetHKP : 1, hkp : 10});
    assert.eq(x.ok, 1);
    assert.eq(x.ret, false);

    print("shutting down arbiter");
    replTest.stop(2);
    print("restarting arbiter without replset");
    replTest.restartWithoutReplset(2);
    conns[2].getDB("local").replVote.update({_id : "highestVote"}, {$set : {val : 100}});
    var result = conns[2].getDB("local").runCommand({getLastError:1});
    printjson(result);
    assert.eq(result.err, null);
    x = conns[2].getDB("local").replVote.find().next();
    assert.eq(x._id, "highestVote");
    assert.eq(x.val, 100);
    print("shutting down arbiter");
    replTest.stop(2);
    print("restarting arbiter in replset");
    replTest.restart(2);
    assert.soon(function() {var x = conns[2].getDB("admin").runCommand({replSetGetStatus:1}); return x["members"][2]["state"] == 7});

    // now ensure that an election happens soon
    print("checking status of 2\n");
    assert.soon(function() {var x = conns[2].getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); printjson(x["members"][2]); return x["members"][2]["highestKnownPrimaryInReplSet"] > 100});
    print("checking status of 1\n");
    assert.soon(function() {var x = conns[2].getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); printjson(x["members"][1]); return x["members"][1]["highestKnownPrimaryInReplSet"] > 100});
    print("checking status of 0\n");
    assert.soon(function() {var x = conns[2].getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); printjson(x["members"][0]); return x["members"][0]["highestKnownPrimaryInReplSet"] > 100});
    print("checking highestVote on master\n");
    master = replTest.getMaster();
    x = master.getDB("local").replVote.find().next();
    assert.eq(x._id, "highestVote");
    assert(x.val > 100);

    print("highestKnownPrimary.js SUCCESS");
    replTest.stopSet(signal);
};

doTest(15, 31000, 1000000);

