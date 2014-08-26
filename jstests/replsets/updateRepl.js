
function dbs_match(a, b) {
    print("dbs_match");

    var ac = a.system.namespaces.find().sort({name:1}).toArray();
    var bc = b.system.namespaces.find().sort({name:1}).toArray();
    if (ac.length != bc.length) {
        print("dbs_match: namespaces don't match, lengths different");
        print("\n\n");
        printjson(ac);
        print("\n\n");
        printjson(bc);
        print("\n\n");
        return false;
    }
    for (var i = 0; i < ac.length; i++) {
        if (ac[i].name != bc[i].name) {
            print("dbs_match: namespaces don't match");
            print("\n\n");
            printjson(ac);
            print("\n\n");
            printjson(bc);
            print("\n\n");
            return false;
        }
    }

    var c = a.getCollectionNames();
    for( var i in c ) {
        print("checking " + c[i]);
        if( !friendlyEqual( a[c[i]].find().sort({_id:1}).toArray(), b[c[i]].find().sort({_id:1}).toArray() ) ) { 
            print("dbs_match: collections don't match " + c[i]);
            return false;
        }
    }
    return true;
};


// check last entry is a full update with pre-image and post-image
verifyLastEntry = function (conn, expectedOp) {
    lastEntry = conn.getDB("local").oplog.rs.find().sort({_id:-1}).next();
    printjson(lastEntry);
    assert(lastEntry["ops"][0]["op"] == expectedOp);
}


doTest = function (signal, txnLimit, startPort, fastup) {
    var num = 3;
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

    if (fastup) {
        assert.commandWorked(conns[0].getDB("admin").runCommand({ setParameter: 1, fastupdates: true }));
    }

    // make our collections
    var db = conns[0].getDB("foo");
    var db2 = conns[1].getDB("foo");
    assert.commandWorked(db.createCollection("foo"));
    db.foo.ensureIndex({b:1});
    assert.commandWorked(db.createCollection("fooCK"));
    db.fooCK.ensureIndex({b:1}, {clustering : true});
    assert.commandWorked(db.createCollection("fooPK", {primaryKey : {a : 1, _id : 1}}));
    assert.commandWorked(db.createCollection("fooCapped", {capped : 1, size : 11111}));

    // insert some data into each collection
    for (var i = 0; i < 10; i++) {
        db.foo.insert({_id : i, a : 10*i, b : 100*i, c : 1000*i});
        db.fooPK.insert({_id : i, a : 10*i, b : 100*i, c : 1000*i});
        db.fooCapped.insert({_id : i, a : 10*i, b : 100*i, c : 1000*i});
    }
    replTest.awaitReplication();

    // now let's start the test

    // verify that cases where we should be logging full pre-image and full post-image are working

    // normal update without mods
    db.foo.update({_id : 0}, {a : 51, b : 501, c : 5001});
    replTest.awaitReplication();
    verifyLastEntry(conns[1], "u");
    // unindexed capped update
    db.fooCapped.update({_id : 0}, {$inc : {c : 1}});
    replTest.awaitReplication();
    verifyLastEntry(conns[1], "u");
    
    // TODO: dynamic array case

    // cases where we log full pre-image, updates-511.js tests that data is properly replicated

    // indexed update
    db.foo.update({_id : 0}, {$inc : {b : 1}});
    replTest.awaitReplication();
    verifyLastEntry(conns[1], "ur");
    db.foo.update({b : 100}, {$inc : {b : 1}});
    replTest.awaitReplication();
    verifyLastEntry(conns[1], "ur");
    db.fooPK.ensureIndex({b:1});
    db.getLastError();
    db.fooPK.update({a : 20, _id : 2}, {$inc : {b : 1}});
    replTest.awaitReplication();
    verifyLastEntry(conns[1], "ur");
    // pk update
    db.fooPK.update({_id : 0}, {$inc : {a:1}});
    replTest.awaitReplication();
    verifyLastEntry(conns[1], "ur");
    // has a clustering key
    db.fooCK.update({_id : 0}, {$inc : {c : 1}});
    replTest.awaitReplication();
    verifyLastEntry(conns[1], "ur");
    // pk update test

    // case where we don't log full pre-image

    // un-indexed update
    db.foo.update({_id : 0}, {$inc : {c : 1}});
    replTest.awaitReplication();
    verifyLastEntry(conns[1], "up");

    // check data
    assert( dbs_match(db,db2), "server data sets do not match after rollback, something is wrong");

    //add test case where we don't log full pre-image, on secondary it is an indexed update, and we still work right
    assert.commandWorked(db.createCollection("bar"));
    db.bar.insert({_id:0, a:0});
    db.bar.insert({_id:1, a:1});
    replTest.awaitReplication();
    // restart secondary out of replica set
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary without replset");
    replTest.restartWithoutReplset(1);
    conns[1].getDB("foo").bar.ensureIndex({a:1});
    assert(!conns[1].getDB("foo").getLastError());
    // bring it back up
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary in replset");
    replTest.restart(1);
    assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });

    // now do an indexed update
    db.bar.update({_id:0} , { $inc : {a : 100}});
    replTest.awaitReplication();
    verifyLastEntry(conns[1], "up");
    // no verify that we can read the right value out of the index
    conns[1].setSlaveOk();
    plan = conns[1].getDB("foo").bar.find({a : 100}).hint({a:1}).explain();
    printjson(plan);
    assert.eq("IndexCursor a_1", plan.cursor);
    doc = conns[1].getDB("foo").bar.find({a : 100}).hint({a:1}).next();
    assert.eq(doc._id , 0);

    print("updateRepl.js SUCCESS");
    replTest.stopSet(signal);
};

doTest(15, 1000000, 31000, true);
doTest(15, 1000000, 31000, false);


