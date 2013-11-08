// Test that initial sync's code of applying missing ops and filling gaps works correctly

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
}

doTest = function (signal, txnLimit, startPort) {
    var replTest = new ReplSetTest({ name: 'unicomplex', nodes: 3, startPort:startPort, txnMemLimit: txnLimit});
    var nodes = replTest.nodeList();

    var conns = replTest.startSet();
    var r = replTest.initiate({ "_id": "unicomplex",
                              "members": [
                                          { "_id": 0, "host": nodes[0], priority:10 },
                                          { "_id": 1, "host": nodes[1] },
                                          { "_id": 2, "host": nodes[2], arbiterOnly: true}]
                              });

    // Make sure we have a master
    var master = replTest.getMaster();

    print("inserting 10000 documents to master");
    var a = master.getDB("foo");
    a.foo.insert({_id:1, a:1});
    replTest.awaitReplication();

    // stop secondary, bring it up outside of repl set
    // and add index
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary without replset");
    replTest.restartWithoutReplset(1);
    print("adding index on secondary");
    var foodb = conns[1].getDB("foo");
    foodb.foo.ensureIndex({a:1});
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary in replset");
    replTest.restart(1);
    assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });

    a.foo.ensureIndex({a:1});
    replTest.awaitReplication();
    assert( dbs_match(master.getDB("foo"), conns[1].getDB("foo")), "server data sets do not match for db foo, something is wrong");

    // stop secondary, bring it up outside of repl set
    // and add index
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary without replset");
    replTest.restartWithoutReplset(1);
    print("dropping index on secondary");
    foodb = conns[1].getDB("foo");
    foodb.foo.dropIndex("a");
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary in replset");
    replTest.restart(1);
    assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });

    a.foo.dropIndex("a");
    replTest.awaitReplication();
    assert( dbs_match(master.getDB("foo"), conns[1].getDB("foo")), "server data sets do not match for db foo, something is wrong");

    replTest.stopSet(signal);
}

doTest( 15, 1000000, 31000 );
doTest( 15, 1, 41000 );

