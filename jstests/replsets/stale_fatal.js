

doTest = function (signal, txnMemLimit, startPort) {

    var num = 3;
    var host = getHostName();
    var name = "rollback_simple";
    var timeout = 60000;

    var replTest = new ReplSetTest( {name: name, nodes: num, startPort:startPort, txnMemLimit: txnMemLimit} );
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

    //deb(master);

    // Make sure we have an arbiter
    assert.soon(function () {
        res = conns[2].getDB("admin").runCommand({ replSetGetStatus: 1 });
        return res.myState == 7;
    }, "Arbiter failed to initialize.");

    // Wait for initial replication
    var a = conns[0].getDB("foo");
    var b = conns[1].getDB("foo");

    // do some insertion
    for (var i = 0; i < 1000; i++) {
        a.foo.insert({a:i});
    }
    replTest.awaitReplication();
    
    print("shutting down conn1");
    replTest.stop(1);


    for (var i = 0; i < 10; i++) {
        a.foo.insert({a:i});
    }
    var lastID = conns[0].getDB("local").oplog.rs.find().sort({$natural:-1}).limit(-1).next()._id;
    for (var i = 10; i < 1000; i++) {
        a.foo.insert({a:i});
    }

    // now we have inserted a bunch of data that is not on b
    // let's purge some entries so b will go stale when we bring it up
    conns[0].getDB("local").oplog.rs.remove({_id: {$lte:lastID} });
    
    // this should invoke the createCollection to try to rollback, and cause the slave to go fatal,
    print("restarting conn1, and waiting for it to go fatal");
    replTest.restart(1);
    assert.soon(function() { var status = conns[1].getDB("admin").runCommand("replSetGetStatus"); print("status.myState is " + status.myState); return (status.myState == 4);});

    print("stale_fatal.js SUCCESS");
    replTest.stopSet(signal);
};

print("stale_fatal.js");

doTest( 15, 1000000, 31000 );
doTest( 15, 0, 41000 );

