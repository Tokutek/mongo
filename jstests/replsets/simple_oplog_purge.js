// Test that initial sync's code of applying missing ops and filling gaps works correctly


doTest = function (signal, startPort, startingExpireOplogHours) {
    var replTest = new ReplSetTest({ name: 'unicomplex', nodes: 1, startPort:startPort});
    var nodes = replTest.nodeList();

    var conns = replTest.startSet();
    var r = replTest.initiate({ "_id": "unicomplex",
                              "members": [
                                          { "_id": 0, "host": nodes[0], priority:10 }]
                              });

    // Make sure we have a master
    var master = replTest.getMaster();

    replTest.stop(0);
    replTest.restart(0, {expireOplogHours : startingExpireOplogHours, expireOplogDays : "0"});
    master = replTest.getMaster();
    var a = master.getDB("foo");
    a.foo.insert({_id:0, a:1});

    // get first entry
    var localdb = master.getDB("local");
    var c = localdb.oplog.rs.find();
    assert(c.hasNext());
    var entryID = c.next()._id;

    // should be an hour and a half in the past
    var older = new Date();
    older.setTime(older.valueOf() - 150*60000); // 2.5 hours in the past

    print("updating ts of first entry");
    localdb.oplog.rs.update({_id: entryID}, {ts:older});
    c = localdb.oplog.rs.find();
    assert(c.hasNext());
    print(" first oplog entry: " + c.next().ts);

    // now, if we change the value of expireOplogHours to 1, we should
    // see entryID get purged soon
    master.getDB("admin").runCommand({replSetExpireOplog:1, expireOplogDays: 0, expireOplogHours: 1});
    assert.soon(function() { var first = master.getDB("local").oplog.rs.find().next()._id; return (entryID != first) } );

    replTest.stopSet(signal);
}

doTest( 15, 31000, "0" );
doTest( 15, 41000, "10" );

