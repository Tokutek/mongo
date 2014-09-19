
// tests that cloning the oplog properly gives the same partitions

doTest = function (signal, startPort, txnLimit) {
    var num = 5;
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
             {_id:2, host : host+":"+port[2]},
             {_id:3, host : host+":"+port[3]},
             {_id:4, host : host+":"+port[4], priority:0},
            ],
             };

    replTest.initiate(config);
    replTest.awaitReplication();
    assert.soon(function() {return conns[0].getDB("admin").isMaster().ismaster})

    // some dummy inserts
    conns[0].getDB("test").foo.insert({_id:1});
    conns[0].getDB("test").foo.insert({_id:2});
    conns[0].getDB("test").foo.insert({_id:3});
    conns[0].getDB("test").foo.insert({_id:4});
    replTest.awaitReplication();

    assert.soon(function() {var x = conns[0].getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][4]["state"] == 2});
    assert.soon(function() {var x = conns[0].getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][3]["state"] == 2});
    assert.soon(function() {var x = conns[0].getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][2]["state"] == 2});
    assert.soon(function() {var x = conns[0].getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][1]["state"] == 2});
    assert.soon(function() {var x = conns[0].getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][0]["state"] == 1});

    // now restart 0
    replTest.stop(0);
    replTest.restart(0);

    assert.soon(function() {var x = conns[4].getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][4]["state"] == 2});
    assert.soon(function() {var x = conns[4].getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][3]["state"] == 2});
    assert.soon(function() {var x = conns[4].getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][2]["state"] == 2});
    assert.soon(function() {var x = conns[4].getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][1]["state"] == 2});
    assert.soon(function() {var x = conns[4].getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][0]["state"] == 1});

    print("priority.js SUCCESS");
    replTest.stopSet(signal);
};


doTest(15, 31000, 1000000);

