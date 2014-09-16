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

function verifyRollbackSuccessful(pri, sec) {
        // now do verifications
        var a = pri.getDB("test");
        var b = sec.getDB("test");
        assert( dbs_match(a,b), "server data sets do not match after rollback, something is wrong");
        assert.commandFailed(sec.getDB("local").runCommand({'_collectionsExist': ['local.rollback.gtidset']}));
        assert.commandFailed(sec.getDB("local").runCommand({'_collectionsExist': ['local.rollback.docs']}));
        assert.commandWorked(sec.getDB("local").runCommand({'_collectionsExist': ['local.rollback.opdata']}));
}

// this function is used by many tests to test rollback scenarios. The basics are as follows.
// A 3 node replica set is started with a primary, secondary, arbiter. Some data is loaded
// via preloadFunction. Then, we transition the secondary into maintenance mode and load
// a bunch more data into the primary via persistentFunction. Then, on the secondary that
// is in maintenance mode, we run _setGod so we can perform writes, via rollbackFunction.
// The purpose of rollbackFunction is to do work that will be rolled back once we exit
// maintenance mode. We then either verify that data on the primary and secondary are identical,
// hence testing rollback, or verify that the secondary transitioned to fatal.
// Should a test want to check the state of the secondary after rollback
// has occurred, it can provide a checkAftermath function
//
// IMPORTANT POINT: persistentFunction should do a LOT more work than rollbackFunction,
// otherwise rollback will think that the secondary is ahead and eventually the work done
// by persistentFunction is rolled back and not the work done by rollbackFunction. So,
// in all tests, you will see persistentFunction do a lot more work, which causes the GTID
// of the primary to be ahead of the GTID of the secondary
doRollbackTest = function (signal, txnLimit, startPort, preloadFunction, persistentFunction, rollbackFunction, goFatal, checkAftermath) {
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
    
    // now bring secondary out of maintenance, and rollback should occur
    assert.commandWorked(conns[1].getDB("admin").runCommand({replSetMaintenance : 0}));

    if (goFatal) {
        assert.soon(function() { var status = conns[1].getDB("admin").runCommand("replSetGetStatus"); print("status.myState is " + status.myState); return (status.myState == 4);});
    }
    else {
        replTest.awaitReplication();
        assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });
        verifyRollbackSuccessful(conns[0],conns[1]);
        if (checkAftermath) {
            checkAftermath(conns[1]);
        }
    }
    
    print("rollback_unit.js SUCCESS");
    replTest.stopSet(signal);
};


