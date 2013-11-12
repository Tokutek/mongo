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

function undo_and_redo_entries(replTest, conns, txnLimit) {
    var master = replTest.getMaster();
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary without replset");
    replTest.restartWithoutReplset(1);
    var localdb = conns[1].getDB("local");
    var admindb = conns[1].getDB("admin");

    var c = localdb.oplog.rs.find().sort({$natural:-1});
    // the 5 below is arbitrary
    for (i = 0; i < 5; i++) {
        assert(c.hasNext());
        c.next();
    }
    var entryID = c.next()._id;

    // now let's undo this entry
    print("undoing, but keeping entry " + entryID.hex());
    r = admindb.runCommand({replUndoOplogEntry:1, GTID : entryID, keepEntry:1});
    assert.eq(r.ok, 1);
    // the 5 below is arbitrary
    for (i = 0; i < 5; i++) {
        assert(c.hasNext());
        c.next();
    }
    entryID = c.next()._id;
    // set minLive and minUnapplied GTID to be something sensible now
    print("undoing " + entryID.hex());
    r = admindb.runCommand({replUndoOplogEntry:1, GTID : entryID});
    print("running logReplInfo " + entryID.hex());
    r = admindb.runCommand({logReplInfo:1, minLiveGTID : entryID, minUnappliedGTID : entryID});
    assert.eq(r.ok, 1);

    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary with replset and fastsync");
    replTest.restart(1, {fastsync : "", txnMemLimit : txnLimit});
    print("wait to become secondary");
    assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });
    conns[1].setSlaveOk();
    assert( dbs_match(master.getDB("foo"), conns[1].getDB("foo")), "server data sets do not match for db foo, something is wrong");
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
    for (i=0; i < 10000; i++) {
        a.foo.insert({_id:i, a:1});
    }
    replTest.awaitReplication();

    undo_and_redo_entries(replTest, conns, txnLimit);

    print("updating 10000 documents on master");
    for (i=0; i < 10000; i++) {
        a.foo.update({_id:i}, {b:1});
    }
    replTest.awaitReplication();

    undo_and_redo_entries(replTest, conns, txnLimit);

    print("removing 10000 documents from master");
    for (i=0; i < 10000; i++) {
        a.foo.remove({_id:i});
    }
    replTest.awaitReplication();
    undo_and_redo_entries(replTest, conns, txnLimit);

    replTest.stopSet(signal);
}

doTest( 15, 1000000, 31000 );
doTest( 15, 1, 41000 );

