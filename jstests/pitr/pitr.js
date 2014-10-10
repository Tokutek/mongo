
// tests that cloning the oplog properly gives the same partitions

doTest = function (signal, startPort, txnLimit, testGTID) {
    var num = 3;
    var host = getHostName();
    var name = "pitr";
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

    // load the plugin
    var pluginExists = conns[0].getDB("admin").runCommand({loadPlugin: 'pitr_plugin'});
    printjson(pluginExists);
    assert(pluginExists.ok);
    pluginExists = conns[1].getDB("admin").runCommand({loadPlugin: 'pitr_plugin'});
    printjson(pluginExists);
    assert(pluginExists.ok);

    // insert some data
    var a = conns[0].getDB("foo");
    var aLoc = conns[0].getDB("local");
    var aAdmin = conns[0].getDB("admin");
    var b = conns[1].getDB("foo");
    var bLoc = conns[1].getDB("local");
    var bAdmin = conns[1].getDB("admin");
    
    assert.commandWorked(a.createCollection("foo"));
    for (var i = 0; i < 5; i++) {
        a.foo.insert({_id : 5 + i});
    }
    replTest.awaitReplication();
    // get an oplog entry that exists on both master and slave
    var oldEntry = aLoc.oplog.rs.find().sort({$natural:-1}).next();
    sleep(1000);
    for (var i = 0; i < 10; i++) {
        a.foo.insert({_id : i});
    }
    replTest.awaitReplication();

    // make sure that PIR fails if the machine is in primary or secondary
    var time = new Date();
    time.setTime(time.valueOf() + 100000); // 1.5 hours in the past
    assert.commandFailed(aAdmin.runCommand({recoverToPoint : 1, ts:time}));
    assert.commandFailed(bAdmin.runCommand({recoverToPoint : 1, ts:time}));

    // now put the secondary in maintenance mode
    assert.commandWorked(conns[1].getDB("admin").runCommand({replSetMaintenance : 1}));
    for (var i = 0; i < 5; i++) {
        a.foo.insert({_id : 10 + i});
        x = aLoc.oplog.rs.find().sort({$natural:-1}).next();
    }
    assert.eq(a.foo.count(), 15);
    var recoverPoint = aLoc.oplog.rs.find().sort({$natural:-1}).next();
    sleep(1000);
    for (var i = 0; i < 5; i++) {
        a.foo.insert({_id : 15 + i});
        x = aLoc.oplog.rs.find().sort({$natural:-1}).next();
    }
    var newestEntry = aLoc.oplog.rs.find().sort({$natural:-1}).next();

    // now some failure cases of recoverToPointTo

    // must have either a GTID or a timestamp
    assert.commandFailed(bAdmin.runCommand({recoverToPoint : 1})); // need ts or gtid
    assert.commandFailed(bAdmin.runCommand({recoverToPoint : 1, gtid:1})); // need valid gtid
    assert.commandFailed(bAdmin.runCommand({recoverToPoint : 1, ts : "asdf"})); // need valid ts
    assert.commandFailed(bAdmin.runCommand({recoverToPoint : 1, ts : new Date (2012, 1, 1)})); // need ts that is ahead of what we have in oplog
    assert.commandFailed(bAdmin.runCommand({recoverToPoint : 1, gtid:1})); // need valid gtid
    assert.commandFailed(bAdmin.runCommand({recoverToPoint : 1, gtid:newestEntry._id, ts: newestEntry.ts})); // cannot have both ts and gtid
    assert.commandFailed(bAdmin.runCommand({recoverToPoint : 1, gtid:oldEntry._id})); // can't have GTID that is less than what is in oplog
    assert.commandFailed(bAdmin.runCommand({recoverToPoint : 1, gtid:oldEntry.ts})); // can't have GTID that is less than what is in oplog

    // now run a test of pir
    if (testGTID) {
        assert.commandWorked(bAdmin.runCommand({recoverToPoint : 1, gtid:recoverPoint._id}));
    }
    else {
        assert.commandWorked(bAdmin.runCommand({recoverToPoint : 1, ts:recoverPoint.ts}));
    }
    // now verify that things are good
    var lastEntryAfterRecovery = bLoc.oplog.rs.find().sort({$natural:-1}).next();
    print("lastEntryAfterRecovery: ");
    printjson(lastEntryAfterRecovery);
    print("recoverPoint: ");
    printjson(recoverPoint);

    assert(friendlyEqual(lastEntryAfterRecovery._id, recoverPoint._id));
    assert(friendlyEqual(lastEntryAfterRecovery.h, recoverPoint.h));
    assert(friendlyEqual(lastEntryAfterRecovery["ops"], recoverPoint["ops"]));

    // restart out of replset
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary without replset");
    replTest.restartWithoutReplset(1);
    assert.eq(conns[1].getDB("foo").foo.count(), 15);
    assert.eq(conns[1].getDB("foo").foo.find().sort({$natural:-1}).next()._id, 14);

    // bring it back up
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary in replset");
    replTest.restart(1);
    assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });

    replTest.stopSet(signal);

    print("doTest succeds");
};

// simple test that if rollback is required, PIR fails 
doRollbackTest = function (signal, startPort) {
    var num = 3;
    var host = getHostName();
    var name = "pir";
    var timeout = 60000;

    var replTest = new ReplSetTest( {name: name, nodes: num, startPort:startPort, txnMemLimit: 1000000} );
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

    // load the plugin
    var pluginExists = conns[1].getDB("admin").runCommand({loadPlugin: 'pitr_plugin'});
    printjson(pluginExists);
    if (!pluginExists.ok) {
        print("plugin does not exist, must be community, returning");
        replTest.stopSet(signal);
        return;
    }

    conns[0].setSlaveOk();
    conns[1].setSlaveOk();

    // insert some data
    var a = conns[0].getDB("foo");
    var aLoc = conns[0].getDB("local");
    var aAdmin = conns[0].getDB("admin");
    var b = conns[1].getDB("foo");
    var bLoc = conns[1].getDB("local");
    var bAdmin = conns[1].getDB("admin");
    
    assert.commandWorked(a.createCollection("foo"));
    for (var i = 0; i < 10; i++) {
        a.foo.insert({_id : i});
    }
    replTest.awaitReplication();

    // now put the secondary in maintenance mode
    assert.commandWorked(conns[1].getDB("admin").runCommand({replSetMaintenance : 1}));
    for (var i = 0; i < 5; i++) {
        a.foo.insert({_id : 10 + i});
        x = aLoc.oplog.rs.find().sort({$natural:-1}).next();
    }
    var recoverPoint = aLoc.oplog.rs.find().sort({$natural:-1}).next();
    for (var i = 0; i < 5; i++) {
        a.foo.insert({_id : 15 + i});
        x = aLoc.oplog.rs.find().sort({$natural:-1}).next();
    }

    // restart out of replset
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary without replset");
    replTest.restartWithoutReplset(1);
    // now change the last entry in the oplog to ensure
    // that rollback will be required
    var lastID = conns[1].getDB("local").oplog.rs.find().sort({$natural : -1}).next()._id;
    // increase the hash of the last entry, making rollback necessary
    conns[1].getDB("local").oplog.rs.update({_id : lastID}, {$inc : {h : 1}})

    // bring it back up
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary in replset");
    replTest.restart(1, { rsMaintenance : ""});
    // sleep for 30 seconds, the max time it would normally take to start up
    print("starting sleep for 30 seconds\n");
    sleep(30000);
    print("done sleep for 30 seconds\n");
    assert(!conns[1].getDB("admin").isMaster().secondary);
    // now verify that point in time recovery fails
    assert.commandFailed(conns[1].getDB("admin").runCommand({recoverToPoint : 1, gtid : recoverPoint._id}));

    print("doRollbackTest succeds");

    replTest.stopSet(signal);
};


doTest(15, 31000, 1000000, true);
doTest(15, 41000, 1000000, false);
doTest(15, 31000, 1, true);
doTest(15, 41000, 1, false);
doRollbackTest(15, 31000);
print("pir.js SUCCESS");

