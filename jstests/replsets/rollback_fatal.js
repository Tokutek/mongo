

doTest = function (signal, txnLimit, startPort) {

    var num = 3;
    var host = getHostName();
    var name = "rollback_simple";
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
    conns[0].setSlaveOk();

    //deb(master);

    // Make sure we have an arbiter
    assert.soon(function () {
        res = conns[2].getDB("admin").runCommand({ replSetGetStatus: 1 });
        return res.myState == 7;
    }, "Arbiter failed to initialize.");

    // Wait for initial replication
    var a = conns[0].getDB("foo");
    var b = conns[1].getDB("foo");

    // do a simple insertion
    a.foo.insert({a:1});

    print("shutting down conn 0");
    replTest.stop(0);
    print("waiting for conn 1 to become master");
    assert.soon(function() { return conns[1].getDB("admin").isMaster().ismaster; });

    print("do Items to Rollback");
    // when this is attempted to be rolled back, it will cause us to go fatal
    b.createCollection("bar");
    print("shutting down conn1");
    replTest.stop(1);


    print("shutting down conn1");
    replTest.stop(1);
    print("restarting conn0");
    replTest.restart(0);
    print("waiting for conn 0 to become master");
    assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });

    for (var i = 0; i < 1000; i++) {
        conns[0].getDB("foo").foo.insert({_id:i});
    }
    
    // this should invoke the createCollection to try to rollback, and cause the slave to go fatal,
    print("restarting conn1, and waiting for it to go fatal");
    replTest.restart(1);
    assert.soon(function() { var status = conns[1].getDB("admin").runCommand("replSetGetStatus"); print("status.myState is " + status.myState); return (status.myState == 4);});

    print("rollback_fatal.js SUCCESS");
    replTest.stopSet(signal);
};

print("rollback_fatal.js");

doTest( 15, 1000000, 31000 );
doTest( 15, 0, 41000 );

