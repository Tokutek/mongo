// rollback of individual entries. This is basically a unit test

restartSlaveOutOfReplset = function (replTest) {
    // stop secondary, bring it up outside of repl set
    // and add index
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary without replset");
    replTest.restartWithoutReplset(1);
}

restartSlaveInReplset = function(replTest, conns) {
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary in replset");
    replTest.restart(1);
    assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });
}

getLastGTIDInOplog = function (conns) {
    var localdb = conns[1].getDB("local");
    var c = localdb.oplog.rs.find().sort({$natural:-1});
    assert(c.hasNext());
    var entryID = c.next()._id;
    return entryID;
}

getLastRefInOplog = function (conns) {
    var localdb = conns[1].getDB("local");
    var c = localdb.oplog.rs.find().sort({$natural:-1});
    assert(c.hasNext());
    var refID = c.next().ref;
    return refID;
}

verifyLastItemCannotBeRolledBack = function (replTest, conns) {
    replTest.awaitReplication();
    restartSlaveOutOfReplset(replTest);

    var admindb = conns[1].getDB("admin");
    var entryID = getLastGTIDInOplog(conns);
    print("trying to rollback an item");
    assert.commandFailed(admindb.runCommand({replUndoOplogEntry:1, GTID : entryID}));

    restartSlaveInReplset(replTest, conns);
}

rollbackLastItem = function (conns, txnLimit) {
    var entryID = getLastGTIDInOplog(conns);
    var refID = getLastRefInOplog(conns);
    var admindb = conns[1].getDB("admin");
    print("rollback last item " + entryID);
    if (txnLimit == 1) {
        var num = conns[1].getDB("local").oplog.refs.find({"_id.oid" : refID}).count();
        assert(num > 0);
    }
    assert.commandWorked(admindb.runCommand({replUndoOplogEntry:1, GTID : entryID}));
    assert.eq(0, conns[1].getDB("local").oplog.rs.find({_id:entryID}).count());
    if (txnLimit == 1) {
        var num = conns[1].getDB("local").oplog.refs.find({"_id.oid":refID}).count();
        assert.eq(0, num);
    }
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

    var a = master.getDB("foo");
    a.createCollection("foo");
    replTest.awaitReplication();
    verifyLastItemCannotBeRolledBack(replTest, conns);

    a.foo.drop();
    verifyLastItemCannotBeRolledBack(replTest, conns);

    a.foo.insert({a:1});
    a.foo.ensureIndex({a:1});
    verifyLastItemCannotBeRolledBack(replTest, conns);
    a.foo.dropIndex("a");
    verifyLastItemCannotBeRolledBack(replTest, conns);

    a.foo.insert({_id:0});
    replTest.awaitReplication();
    restartSlaveOutOfReplset(replTest);
    assert.eq(1, conns[1].getDB("foo").foo.find({_id:0}).count());
    rollbackLastItem(conns, txnLimit);
    assert.eq(0, conns[1].getDB("foo").foo.find({_id:0}).count());
    restartSlaveInReplset(replTest, conns);

    a.foo.insert({_id:1, a:1});
    a.foo.update({_id:1}, {a:2});
    replTest.awaitReplication();
    restartSlaveOutOfReplset(replTest);
    var currDoc = conns[1].getDB("foo").foo.find({_id:1}).next();
    assert.eq(2, currDoc.a);
    rollbackLastItem(conns, txnLimit);
    currDoc = conns[1].getDB("foo").foo.find({_id:1}).next();
    assert.eq(1, currDoc.a);
    restartSlaveInReplset(replTest, conns);

    a.foo.insert({_id:2});
    a.foo.remove({_id:2});
    replTest.awaitReplication();
    restartSlaveOutOfReplset(replTest);
    assert.eq(0, conns[1].getDB("foo").foo.find({_id:2}).count());
    rollbackLastItem(conns, txnLimit);
    assert.eq(1, conns[1].getDB("foo").foo.find({_id:2}).count());
    restartSlaveInReplset(replTest, conns);    
    
    a.foo.insert([{_id:6},{_id:5},{_id:4},{_id:3}]);
    replTest.awaitReplication();
    restartSlaveOutOfReplset(replTest);
    assert.eq(4, conns[1].getDB("foo").foo.find({_id:{$gt:2}}).count());
    rollbackLastItem(conns, txnLimit);
    assert.eq(0, conns[1].getDB("foo").foo.find({_id:{$gt:2}}).count());
    restartSlaveInReplset(replTest, conns);    
    
    
    replTest.stopSet(signal);
}

// if we ever change this, need to change rollbackLastItem, which uses the value of 1
doTest( 15, 1, 41000 );
doTest( 15, 1000000, 31000 );

