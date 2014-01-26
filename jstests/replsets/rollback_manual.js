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

    print('Testing insert rollback');
    a.foo.insert({_id:0});
    replTest.awaitReplication();
    restartSlaveOutOfReplset(replTest);
    assert.eq(1, conns[1].getDB("foo").foo.find({_id:0}).count());
    rollbackLastItem(conns, txnLimit);
    assert.eq(0, conns[1].getDB("foo").foo.find({_id:0}).count());
    restartSlaveInReplset(replTest, conns);

    print('Testing capped collection update rollback');
    a.createCollection("bar", {capped:1, size:1000000000});
    a.bar.insert({_id:0, a:0});
    a.bar.update({_id:0}, {a:2});
    replTest.awaitReplication();
    restartSlaveOutOfReplset(replTest);
    x = conns[1].getDB("foo").bar.findOne();
    assert.eq(0, x._id);
    assert.eq(2, x.a);
    rollbackLastItem(conns, txnLimit);
    x = conns[1].getDB("foo").bar.findOne();
    assert.eq(0, x._id);
    assert.eq(0, x.a);
    restartSlaveInReplset(replTest, conns);
    a.bar.update({_id:0}, {$set : {a:3}});
    replTest.awaitReplication();
    restartSlaveOutOfReplset(replTest);
    x = conns[1].getDB("foo").bar.findOne();
    assert.eq(0, x._id);
    assert.eq(3, x.a);
    rollbackLastItem(conns, txnLimit);
    x = conns[1].getDB("foo").bar.findOne();
    assert.eq(0, x._id);
    assert.eq(2, x.a);
    restartSlaveInReplset(replTest, conns);

    print('Testing rollback of update with custom PK');
    a.bar.drop();
    assert.commandWorked(a.runCommand({create: "bar", primaryKey : {a:1, _id : 1}}));
    a.bar.insert({_id:0, a:0, b:0});
    a.bar.update({_id:0}, {a:2}); // test update that changes PK
    restartSlaveOutOfReplset(replTest);
    x = conns[1].getDB("foo").bar.findOne();
    assert.eq(0, x._id);
    assert.eq(2, x.a);
    rollbackLastItem(conns, txnLimit);
    x = conns[1].getDB("foo").bar.findOne();
    assert.eq(0, x._id);
    assert.eq(0, x.a);
    restartSlaveInReplset(replTest, conns);
    a.bar.update({_id:0}, {$set : {a:3}});
    replTest.awaitReplication();
    restartSlaveOutOfReplset(replTest);
    x = conns[1].getDB("foo").bar.findOne();
    assert.eq(0, x._id);
    assert.eq(3, x.a);
    rollbackLastItem(conns, txnLimit);
    x = conns[1].getDB("foo").bar.findOne();
    assert.eq(0, x._id);
    assert.eq(2, x.a);
    restartSlaveInReplset(replTest, conns);
    print('Done testing 0\n');
    a.bar.remove({});
    print('Removed everything, testing 1\n');
    a.bar.insert({_id:1, a:1, b:1});
    replTest.awaitReplication();
    x = a.bar.find({_id:1}).next();
    printjson(x);
    a.bar.update({_id:1}, {$set : {b:3}});
    replTest.awaitReplication();
    x = a.bar.find({_id:1}).next();
    printjson(x);
    restartSlaveOutOfReplset(replTest);
    x = conns[1].getDB("foo").bar.find({_id:1}).next();
    assert.eq(1, x._id);
    assert.eq(1, x.a);
    assert.eq(3, x.b);
    rollbackLastItem(conns, txnLimit);
    x = conns[1].getDB("foo").bar.find({_id:1}).next();
    assert.eq(1, x._id);
    assert.eq(1, x.a);
    assert.eq(1, x.b);
    restartSlaveInReplset(replTest, conns);

    print('Testing simple update rollback');
    for (i = 0; i < 2; i++) {
        a.foo.remove({_id:1});
        a.foo.insert({_id:1, a:1});
        a.foo.update({_id:1}, i == 0 ? {a:2} : {$inc:{a:1}});
        replTest.awaitReplication();
        restartSlaveOutOfReplset(replTest);
        var currDoc = conns[1].getDB("foo").foo.find({_id:1}).next();
        assert.eq(2, currDoc.a);
        print("running rollback of update with i " + i);
        rollbackLastItem(conns, txnLimit);
        currDoc = conns[1].getDB("foo").foo.find({_id:1}).next();
        assert.eq(1, currDoc.a);
        restartSlaveInReplset(replTest, conns);
    }

    print('Testing remove rollback');
    a.foo.insert({_id:2});
    a.foo.remove({_id:2});
    replTest.awaitReplication();
    restartSlaveOutOfReplset(replTest);
    assert.eq(0, conns[1].getDB("foo").foo.find({_id:2}).count());
    rollbackLastItem(conns, txnLimit);
    assert.eq(1, conns[1].getDB("foo").foo.find({_id:2}).count());
    restartSlaveInReplset(replTest, conns);    
    
    print('Testing multi-insert rollback');
    a.foo.insert([{_id:6},{_id:5},{_id:4},{_id:3}]);
    replTest.awaitReplication();
    restartSlaveOutOfReplset(replTest);
    assert.eq(4, conns[1].getDB("foo").foo.find({_id:{$gt:2}}).count());
    rollbackLastItem(conns, txnLimit);
    assert.eq(0, conns[1].getDB("foo").foo.find({_id:{$gt:2}}).count());
    restartSlaveInReplset(replTest, conns);    
    
    for (fast in [ true, false ]) {
        print('Testing update rollback 2, fast: ' + fast);
        if (fast) assert.commandWorked(a.getSisterDB('admin').runCommand({ setParameter: 1, fastupdates: true }));
        a.foo.remove({_id: 7});
        a.foo.insert({_id: 7});
        a.foo.update({_id: 7}, { $inc: { c: 1 } });
        replTest.awaitReplication();
        restartSlaveOutOfReplset(replTest);
        assert.eq(1, conns[1].getDB("foo").foo.find({ _id: 7, c: 1 }).count());
        rollbackLastItem(conns, txnLimit);
        if (false) {
            // fastupdate rolls back to { c: 0 }, even though c did not exist before.
            assert.eq({ _id: 7, c: 0 }, conns[1].getDB("foo").foo.find({ _id: 7, c: 0 }).toArray()[0]);
        } else {
            // regular update rolls back to no value for c
            assert.eq({ _id: 7 }, conns[1].getDB("foo").foo.find({ _id: 7 }).toArray()[0]);
        }
        restartSlaveInReplset(replTest, conns);    
        if (fast) assert.commandWorked(a.getSisterDB('admin').runCommand({ setParameter: 1, fastupdates: false }));
    }

    for (fast in [ true, false ]) {
        print('Testing update rollback 3, fast: ' + fast);
        if (fast) assert.commandWorked(a.getSisterDB('admin').runCommand({ setParameter: 1, fastupdates: true }));
        a.foo.remove({_id: 8});
        a.foo.insert({_id: 8, c: 0});
        a.foo.update({_id: 8}, { $inc: { c: 1 } }); assert.eq(null, a.getLastError());
        replTest.awaitReplication();
        restartSlaveOutOfReplset(replTest);
        assert.eq(1, conns[1].getDB("foo").foo.find({ _id: 8, c: 1 }).count());
        rollbackLastItem(conns, txnLimit);
        // rollback sets { c: 0 }, since it was incremented by 1 starting at zero.
        // this is expected for both fastupdates and regular updates.
        assert.eq(1, conns[1].getDB("foo").foo.find({ _id: 8, c: 0 }).count());
        restartSlaveInReplset(replTest, conns);
        if (fast) assert.commandWorked(a.getSisterDB('admin').runCommand({ setParameter: 1, fastupdates: false }));
    }

    replTest.stopSet(signal);
}

// if we ever change this, need to change rollbackLastItem, which uses the value of 1
doTest( 15, 1000000, 31000 );
doTest( 15, 1, 41000 );

