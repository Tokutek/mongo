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

function undo_and_redo_entries(replTest, conns, txnLimit, undoFun) {
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
    var entry = c.next();
    printjson(entry);
    var entryID = entry._id;

    // now let's undo this entry
    print("undoing, but keeping entry " + entryID.printGTID());
    r = admindb.runCommand({replPurgeOplogEntry:1, GTID : entryID});
    assert.eq(r.ok, 1);
    // now undo the operation
    undoFun(conns, false);

    // the 5 below is arbitrary
    for (i = 0; i < 5; i++) {
        assert(c.hasNext());
        c.next();
    }
    entry = c.next();
    printjson(entry);
    entryID = entry._id;
    // set minLive and minUnapplied GTID to be something sensible now
    print("undoing " + entryID.printGTID());
    r = admindb.runCommand({replPurgeOplogEntry:1, GTID : entryID});
    print("running logReplInfo " + entryID.hex());
    r = admindb.runCommand({logReplInfo:1, minLiveGTID : entryID, minUnappliedGTID : entryID, keepEntry:1});
    assert.eq(r.ok, 1);
    // now undo the operation
    undoFun(conns, true);

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

    print("inserting 30 documents to master");
    var a = master.getDB("foo");
    for (i=0; i < 30; i++) {
        a.foo.insert({_id:i, a:1});
    }
    replTest.awaitReplication();

    undoInsert = function(conns, first) {
        if (first) {
            conns[1].getDB("foo").foo.remove({_id : 18});
        }
        else {
            conns[1].getDB("foo").foo.remove({_id : 24});
        }
    };
    undo_and_redo_entries(replTest, conns, txnLimit, undoInsert);
/*
    print("updating 30 documents on master with $inc optimization");
    assert.commandWorked(a.getSisterDB('admin').runCommand({ setParameter: 1, fastupdates: true }))
    for (i=0; i < 30; i++) {
        a.foo.update({_id:10}, {$inc:{a:1}});
    }
    replTest.awaitReplication();
    // even though we are updating the same element above,
    // it should be ok to undo and redo selected entries
    // because of how $inc operates.
    undoUpdate = function(conns, first) {
        if (first) {
            conns[1].getDB("foo").foo.update({_id:10}, {$inc:{a:-1}});
        }
        else {
            conns[1].getDB("foo").foo.update({_id:10}, {$inc:{a:-1}});
        }
    };
    undo_and_redo_entries(replTest, conns, txnLimit, undoUpdate);
*/
    print("updating 30 documents on master");
    for (i=0; i < 30; i++) {
        a.foo.update({_id:i}, {b:1});
    }
    replTest.awaitReplication();

    undoUpdate2 = function(conns, first) {
        if (first) {
            conns[1].getDB("foo").foo.update({_id:18}, {a : 1});
        }
        else {
            conns[1].getDB("foo").foo.update({_id:24}, {a : 1});
        }
    };
    undo_and_redo_entries(replTest, conns, txnLimit, undoUpdate2);

    print("removing 30 documents from master");
    for (i=0; i < 30; i++) {
        a.foo.remove({_id:i});
    }
    replTest.awaitReplication();
    undoRemove = function(conns, first) {
        if (first) {
            conns[1].getDB("foo").foo.insert({_id:18, b : 1});
        }
        else {
            conns[1].getDB("foo").foo.insert({_id:24, b : 1});
        }
    };
    undo_and_redo_entries(replTest, conns, txnLimit, undoRemove);

    print("SUCCESS\n");
    replTest.stopSet(signal);
}

doTest( 15, 1000000, 31000 );
doTest( 15, 1, 41000 );
