
// tests that cloning the oplog properly gives the same partitions

doTest = function (signal, startPort, txnLimit) {
    var num = 3;
    var host = getHostName();
    var name = "consensus";
    var timeout = 60000;

    var replTest = new ReplSetTest( {name: name, nodes: num, startPort:startPort, txnMemLimit: txnLimit} );
    var conns = replTest.startSet();
    var port = replTest.ports;
    var config = {_id : name, members :
            [
             {_id:0, host : host+":"+port[0], priority:10 },
             {_id:1, host : host+":"+port[1]},
             {_id:2, host : host+":"+port[2], priority:0},
            ],
             };

    replTest.initiate(config);
    replTest.awaitReplication();
    config = replTest.getReplSetConfig();
    config.version = 10;
try {
    conns[0].getDB("admin").runCommand({replSetReconfig:config});
}
catch(e) {
  print(e);
}
    print("waiting for 0 to become master post reconfig");
    assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });
    replTest.awaitReplication();
    print("starting to bridge");
    var master = replTest.getMaster();
    //assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });

    // some dummy inserts
    master.getDB("test").foo.insert({_id:1});
    master.getDB("test").foo.insert({_id:2});
    master.getDB("test").foo.insert({_id:3});
    master.getDB("test").foo.insert({_id:4});

    replTest.awaitReplication();
    print("dummy inserts done");

    lastGTID = conns[0].getDB("local").oplog.rs.find().sort({$natural : -1}).next()._id;

    // should veto because we see another primary
    print("replSetFresh ought to fail because a primary exists");
    x = conns[2].getDB("admin").runCommand({replSetFresh : 1, set : "consensus", GTID : lastGTID, who : "asdf", cfgver : NumberInt(10), id : NumberInt(1), ignoreElectable : 1});
    assert(x.veto == true);
    print("replSetElect ought to get veto because a primary exists");
    x = conns[2].getDB("admin").runCommand({
        replSetElect : 1, 
        set : "consensus",
        who: "asdf",
        whoid: NumberInt(1),
        cfgver : NumberInt(10),
        round : ObjectId("4dc07fedd8420ab8d0d4066d"), // a dummy
        primaryToUse : 200,
        gtid : lastGTID});
    assert(x.ok == 1);
    assert(x.vote == -10000);

    // create a partition such that 2 is isolated
    replTest.bridge();
    replTest.partition(0,2);
    replTest.partition(1,2);
    // soon, as far as node[2] is concerned, only 2 is a secondary
    print("making sure nodes are in right state");
    var master = replTest.getMaster();
    var secs = replTest.getSecondaries();
    var isolated = secs[1];
    x = isolated.getDB("admin").runCommand({replSetGetStatus:1});
    printjson(x);
    assert.soon(function() {var x = isolated.getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][1]["state"] == 8});
    assert.soon(function() {var x = isolated.getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][0]["state"] == 8});

    //now let's artificially increase the highestKnownPrimaryInSet
    print("_replSetHKP to 100");
    x = isolated.getDB("admin").runCommand({_replSetHKP : 1, hkp : 100});
    assert.eq(x.ok, 1);
    assert.eq(x.ret, true);
    assert.soon(function() {var x = isolated.getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][2]["highestKnownPrimaryInReplSet"] == 100});

    // now let's do targeted testing of the commands that make up elections.
    // the first one being replSetFresh
    // let's make sure that we can get replSetFresh to tell us we should elect
    // ourselves. That is our control
    print("manually running replSetFresh");
    lastGTID = isolated.getDB("local").oplog.rs.find().sort({$natural : -1}).next()._id;
    x = isolated.getDB("admin").runCommand({replSetFresh : 1, set : "consensus", GTID : lastGTID, who : "asdf", cfgver : NumberInt(10), id : NumberInt(1), ignoreElectable : 1});
    printjson(x);
    assert.eq(x.ok, 1);
    assert.eq(x.hkp, 100);
    assert.eq(x.vote, 1);
    assert(x.fresher == false);
    assert(x.veto == false);

    // now test some cases where we will veto

    // config too low
    print("low config causes veto");
    x = isolated.getDB("admin").runCommand({replSetFresh : 1, set : "consensus", GTID : lastGTID, who : "asdf", cfgver : NumberInt(9), id : NumberInt(1), ignoreElectable : 1});
    assert(x.veto == true);
    // now test whether fresher is properly set
    gPri = lastGTID.GTIDPri();
    gSec = lastGTID.GTIDSec();
    print("GTID PRI: " + gPri);
    print("GTID SEC: " + gSec);
    print("checking fresher");
    // using gPri-1 instead of gSec-1 because gSec may be 0
    x = isolated.getDB("admin").runCommand({replSetFresh : 1, set : "consensus", GTID : GTID(gPri-1, gSec), who : "asdf", cfgver : NumberInt(10), id : NumberInt(1), ignoreElectable : 1});
    assert(x.fresher == true);
    x = isolated.getDB("admin").runCommand({replSetFresh : 1, set : "consensus", GTID : GTID(gPri, gSec+1), who : "asdf", cfgver : NumberInt(10), id : NumberInt(1), ignoreElectable : 1});
    assert(x.fresher == false);
    // check bad replset name
    x = isolated.getDB("admin").runCommand({replSetFresh : 1, set : "consensusa", GTID : lastGTID, who : "asdf", cfgver : NumberInt(10), id : NumberInt(1), ignoreElectable : 1});
    assert.eq(x.ok, 0);

    // now let's test replSetElect
    // verify veto cases
    print("replSetElect tests");
    print("low config causes veto");
    x = isolated.getDB("admin").runCommand({
        replSetElect : 1, 
        set : "consensus",
        who: "asdf",
        whoid: NumberInt(1),
        cfgver : NumberInt(9),
        round : ObjectId("4dc07fedd8420ab8d0d4066d"), // a dummy
        primaryToUse : 200,
        gtid : lastGTID});
    assert(x.ok == 1);
    assert(x.vote == -10000);

    // GTID that is too low gets a veto
    print("replSetElect with low GTID gets no vote");
    x = isolated.getDB("admin").runCommand({
        replSetElect : 1, 
        set : "consensus",
        who: "asdf",
        whoid: NumberInt(1),
        cfgver : NumberInt(10),
        round : ObjectId("4dc07fedd8420ab8d0d4066d"), // a dummy
        primaryToUse : 200,
        gtid : GTID(gPri-1, gSec)});
    assert(x.ok == 1);
    assert(x.vote == -10000);

    x = isolated.getDB("local").replVote.find().next();
    assert.eq(x._id, "highestVote");
    assert(x.val < 150);

    print("proper primaryToUse gets vote");
    x = isolated.getDB("admin").runCommand({
        replSetElect : 1, 
        set : "consensus",
        who: "asdf",
        whoid: NumberInt(1),
        cfgver : NumberInt(10),
        round : ObjectId("4dc07fedd8420ab8d0d4066d"), // a dummy
        primaryToUse : 150,
        gtid : lastGTID});
    assert(x.ok == 1);
    assert(x.vote == 1);

    x = isolated.getDB("local").replVote.find().next();
    assert.eq(x._id, "highestVote");
    assert.eq(x.val, 150);

    print("same or lower primaryToUse gets no vote");
    x = isolated.getDB("admin").runCommand({
        replSetElect : 1, 
        set : "consensus",
        who: "asdf",
        whoid: NumberInt(1),
        cfgver : NumberInt(10),
        round : ObjectId("4dc07fedd8420ab8d0d4066d"), // a dummy
        primaryToUse : 150,
        gtid : lastGTID});
    assert(x.ok == 1);
    assert(x.vote == 0);

    x = isolated.getDB("admin").runCommand({
        replSetElect : 1, 
        set : "consensus",
        who: "asdf",
        whoid: NumberInt(1),
        cfgver : NumberInt(10),
        round : ObjectId("4dc07fedd8420ab8d0d4066d"), // a dummy
        primaryToUse : 149,
        gtid : lastGTID});
    assert(x.ok == 1);
    assert(x.vote == 0);
    x = isolated.getDB("local").replVote.find().next();
    assert.eq(x._id, "highestVote");
    assert.eq(x.val, 150);

    // verify that a replSetFresh now returns 150 instead of 100
    print("rerunning replSetFresh now gives us a hkp of 150");
    x = isolated.getDB("admin").runCommand({replSetFresh : 1, set : "consensus", GTID : GTID(gPri, gSec+1), who : "asdf", cfgver : NumberInt(10), id : NumberInt(1), ignoreElectable : 1});
    assert(x.ok == 1);
    assert.eq(x.hkp, 150);
    assert.soon(function() {var x = isolated.getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][2]["highestKnownPrimaryInReplSet"] == 150});
    
    print("consensus.js SUCCESS");
    replTest.stopSet(signal);
};

doTest(15, 31000, 1000000);

