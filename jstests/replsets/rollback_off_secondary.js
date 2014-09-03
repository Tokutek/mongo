// simple test of rollback from the 1.0 days, refactored

var filename;
//if (TestData.testDir !== undefined) {
//    load(TestData.testDir + "/replsets/_rollback_helpers.js");
//} else {
    load('jstests/replsets/_rollback_helpers.js');
//}

function doInitialWrites(conn) {
    var t = conn.getDB("test").bar;
    t.insert({ q:0});
    t.insert({ q: 1, a: "foo" });
    t.insert({ q: 2, a: "foo", x: 1 });
    t.insert({ q: 3, bb: 9, a: "foo" });
    t.insert({ q: 40, a: 1 });
    t.insert({ q: 40, a: 2 });
    t.insert({ q: 70, txt: 'willremove' });
    var db = conn.getDB("test");
    db.createCollection("kap", { capped: true, size: 5000 });
    db.kap.insert({ foo: 1 })

    // going back to empty on capped is a special case and must be tested
    assert.commandWorked(db.createCollection("kap2", { capped: true, size: 5501 }));

    db.createCollection("foo");
}

function doItemsToRollBack(conn) {
    db = conn.getDB("test");
    t = db.bar;
    t.insert({ q: 4 });
    t.update({ q: 3 }, { q: 3, rb: true });

    t.remove({ q: 40 }); // multi remove test

    t.update({ q: 2 }, { q: 39, rb: true });

    // rolling back a delete will involve reinserting the item(s)
    t.remove({ q: 1 });

    t.update({ q: 0 }, { $inc: { y: 1} });

    db.kap.insert({ foo: 2 })
    db.kap2.insert({ foo: 2 })

    db.runCommand("beginTransaction");
    t.insert({q:3});
    t.update({ q: 3 }, { q: 4, rb: true });
    db.runCommand("commitTransaction");
}

preloadLotsMoreData = function(conn) {
    var res = conn.getDB("admin").runCommand({configureFailPoint: 'disableReplInfoThread', mode: 'alwaysOn'});
    assert.eq(1, res.ok, "could not disable repl info thread");

    for (var i = 10; i < 200; i++) {
        conn.getDB("test").foo.insert({_id : i, state : "after split"});
    }
};


// adapted from function found in _rollback_helpers.js
doRollbackTestOffSecondary = function (signal, txnLimit, startPort, preloadFunction, persistentFunction, rollbackFunction) {
    var num = 5;
    var host = getHostName();
    var name = "rollback_unit";
    var timeout = 60000;

    var replTest = new ReplSetTest( {name: name, nodes: num, startPort:startPort, txnMemLimit: txnLimit} );
    var conns = replTest.startSet();
    var port = replTest.ports;
    var config = {_id : name, members :
            [
             {_id:0, host : host+":"+port[0], priority:10 },
             {_id:1, host : host+":"+port[1]},
             {_id:2, host : host+":"+port[2], arbiterOnly : true},
             {_id:3, host : host+":"+port[3], arbiterOnly : true},
             {_id:4, host : host+":"+port[4], arbiterOnly : true},
            ],
             };

    replTest.initiate(config);
    replTest.awaitReplication();
    assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });

    // Make sure we have a master
    conns[0].setSlaveOk();
    conns[1].setSlaveOk();

    // Make sure we have an arbiter
    assert.soon(function () {
        res = conns[2].getDB("admin").runCommand({ replSetGetStatus: 1 });
        return res.myState == 7;
    }, "Arbiter failed to initialize.");

    //create a dummy collection
    preloadFunction(conns[0]);
    replTest.awaitReplication();

    // take secondary into maintenance mode and put it in God mode so we can do insertions
    assert.commandWorked(conns[1].getDB("admin").runCommand({replSetMaintenance : 1}));
    assert.commandWorked(conns[1].getDB("admin").runCommand({_setGod : 1}));
    // verify that local.rollback.opdata does not exist, which it should not
    // because no one has created it yet. preloadFunction, called above,
    // should not have access to this conns[1]
    assert.commandFailed(conns[1].getDB("local").runCommand({'_collectionsExist': ['local.rollback.opdata']}));

    // crank up some insertions into master, so GTID of master is ahead of whatever we do on slave
    persistentFunction(conns[0]);

    // do something to secondary in God mode that will be rolled back
    rollbackFunction(conns[1]);

    // now stop arbiters to make the primary step down
    var x = conns[0].getDB("local").oplog.rs.find().sort({$natural:-1}).next();
    printjson(x);
    var gtid0 = x["_id"];

    print("stopping arbiters");
    replTest.stop(4);
    replTest.stop(3);
    replTest.stop(2);
    print("waiting for primary to step down");
    assert.soon(function() {
        var x = conns[1].getDB("admin").runCommand({replSetGetStatus:1});
        printjson(x);
        return x["members"][0]["state"] == 2; // check that primary is down
    });
    // now bring secondary out of maintenance, and rollback should occur
    print("bringing 1 out of maintenance mode");
    assert.commandWorked(conns[1].getDB("admin").runCommand({replSetMaintenance : 0}));
    print("waiting for rollback to complete");
    assert.soon(function() {
        x = conns[1].getDB("local").oplog.rs.find().sort({$natural:-1}).next();
        printjson(x);
        var gtid1 = x["_id"];
        return friendlyEqual(gtid0, gtid1);
    });
    print("restarting 0");
    replTest.restart(0);
    assert.soon(function() { return conns[0].getDB("admin").isMaster().secondary; });
    conns[0].setSlaveOk();
    print("checking rollback");
    conns[0].setSlaveOk();
    verifyRollbackSuccessful(conns[0],conns[1]);    
    print("rollback_off_secondary.js SUCCESS");
    replTest.stopSet(signal);
};


doRollbackTestOffSecondary( 15, 1000000, 31000, doInitialWrites, preloadLotsMoreData, doItemsToRollBack );
//doRollbackTestOffSecondary( 15, 1, 41000, doInitialWrites, preloadLotsMoreData, doItemsToRollBack );

