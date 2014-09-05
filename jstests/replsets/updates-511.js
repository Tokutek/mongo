
// test that making a key become multikey does not cause an exception to be thrown
// on the secondary (due to not having a write lock)

doNormalUpdates = function(replTest, mfoo, sfoo, slocal, capped ) {
    for (i = 0; i < 1000; i++) {
        mfoo.foo.insert({_id:i, z:1000-i, a:1});
    }
    // do an update with a mod
    mfoo.foo.update({_id:10}, {$set : {a : 1000} });
    replTest.awaitReplication();
    // verify entry in oplog
    x = slocal.oplog.rs.find().sort({$natural:-1}).next();
    printjson(x);
    if (capped) {
        assert.eq(x["ops"][0]["op"], "u"); // verify that it does NOT have mods
    }
    else {
        assert(x["ops"][0]["op"] == "ur"); // verify that it has mods
    }
    // verify data on master
    x = mfoo.foo.find({_id:10}).next();
    assert.eq(x.a, 1000);
    // verify data on slave
    x = sfoo.foo.find({_id:10}).next();
    assert.eq(x.a, 1000);

    // do an update without a mod
    // verify entry in oplog
    mfoo.foo.update({_id:20}, {a: 2000, b : 1000} );
    replTest.awaitReplication();
    x = slocal.oplog.rs.find().sort({$natural:-1}).next();
    printjson(x);
    assert.eq(x["ops"][0]["op"], "u"); // verify that it does not have mods
    // verify data on master
    x = mfoo.foo.find({_id:20}).next();
    assert.eq(x.z, undefined);
    assert.eq(x.b, 1000);
    // verify data on slave
    x = sfoo.foo.find({_id:20}).next();
    assert.eq(x.z, undefined);
    assert.eq(x.b, 1000);
};



doTest = function (signal, startPort, txnLimit) {

    replTest = new ReplSetTest({ name: 'unicomplex', nodes: 3, startPort:startPort, txnMemLimit: txnLimit});
    var nodes = replTest.nodeList();

    var conns = replTest.startSet();
    var r = replTest.initiate({ "_id": "unicomplex",
                              "members": [
                                          { "_id": 0, "host": nodes[0], priority:10  },
                                          { "_id": 1, "host": nodes[1]},
                                          { "_id": 2, "host": nodes[2], arbiterOnly: true}]
                          });

    // Make sure we have a master
    assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });
    var master = conns[0];
    var slave = conns[1];
    slave.setSlaveOk();
    var mfoo = master.getDB("foo");
    var sfoo = slave.getDB("foo");
    var slocal = master.getDB("local");
    // test normal collection, _id index, no secondaries
    mfoo.foo.drop();

    mfoo.createCollection("foo");
    doNormalUpdates(replTest, mfoo, sfoo, slocal, false);
    mfoo.foo.remove({});
    o = { _id : 1 , a : [ { x : 1 , y : 1 } , { x : 2 , y : 2 } , { x : 3 , y : 3 } ] }
    mfoo.foo.insert( o );
    replTest.awaitReplication();
    x = mfoo.foo.findOne();
    assert.eq( o , mfoo.foo.findOne() , "A1" );
    assert.eq( o , sfoo.foo.findOne() , "A1" );

    q = { "a.x" : 2 }
    mfoo.foo.update( q , { $set : { b : 5 } } )
    replTest.awaitReplication();
    o.b = 5
    assert.eq( o , mfoo.foo.findOne() , "A2" )
    assert.eq( o , sfoo.foo.findOne() , "A2" )

    mfoo.foo.update( { "a.x" : 2 } , { $inc : { "a.$.y" : 1 } } )
    replTest.awaitReplication();
    x = slocal.oplog.rs.find().sort({$natural:-1}).next();
    printjson(x);
    assert.eq(x["ops"][0]["op"], "u"); // verify that it does NOT have mods, because it has a dynamic array
    o.a[1].y++;
    assert.eq( o , mfoo.foo.findOne() , "A3" );
    assert.eq( o , sfoo.foo.findOne() , "A3" );

    mfoo.foo.drop();

    mfoo.createCollection("foo");
    mfoo.foo.ensureIndex({a:1});
    doNormalUpdates(replTest, mfoo, sfoo, slocal, false);

    // ensure that the rows we updated with a come back correct
    assert(mfoo.foo.find({a:1000}).explain().cursor == "IndexCursor a_1");
    x = mfoo.foo.find({a:1000}).next();
    assert.eq(x._id, 10);
    assert(mfoo.foo.find({a:2000}).explain().cursor == "IndexCursor a_1");
    x = mfoo.foo.find({a:2000}).next();
    assert.eq(x._id, 20);
    assert(sfoo.foo.find({a:1000}).explain().cursor == "IndexCursor a_1");
    x = sfoo.foo.find({a:1000}).next();
    assert.eq(x._id, 10);
    assert(sfoo.foo.find({a:2000}).explain().cursor == "IndexCursor a_1");
    x = sfoo.foo.find({a:2000}).next();
    assert.eq(x._id, 20);
    mfoo.foo.drop();

    mfoo.createCollection("foo");
    mfoo.foo.ensureIndex({a:1}, {clustering : true});
    doNormalUpdates(replTest, mfoo, sfoo, slocal, false);
    mfoo.foo.drop();

    mfoo.createCollection("foo");
    mfoo.foo.ensureIndex({z:1}, {clustering : true});
    doNormalUpdates(replTest, mfoo, sfoo, slocal, false);
    assert(mfoo.foo.find({z:990}).explain().cursor == "IndexCursor z_1");
    x = mfoo.foo.find({z:990}).next();
    assert.eq(x._id, 10);
    assert.eq(x.a, 1000);
    assert(sfoo.foo.find({z:990}).explain().cursor == "IndexCursor z_1");
    x = sfoo.foo.find({z:990}).next();
    assert.eq(x._id, 10);
    assert.eq(x.a, 1000);
    mfoo.foo.drop();

    mfoo.createCollection("foo", {capped:1, size:1000000000});
    doNormalUpdates(replTest, mfoo, sfoo, slocal, true);
    mfoo.foo.drop();

    assert.commandWorked(mfoo.runCommand({create:"foo", primaryKey: {a : 1, _id : 1}}));
    doNormalUpdates(replTest, mfoo, sfoo, slocal, false);
    mfoo.foo.remove({});
    assert.eq(mfoo.foo.count(), 0);
    mfoo.foo.insert({_id:1, a:1, b:1});
    // cases where pk does not change
    mfoo.foo.update({_id:1, a:1}, {a:1, b:2});
    replTest.awaitReplication();
    x = mfoo.foo.find().next();
    assert(x._id == 1);
    assert(x.a == 1);
    assert(x.b == 2);
    x = sfoo.foo.find().next();
    assert(x._id == 1);
    assert(x.a == 1);
    assert(x.b == 2);

    mfoo.foo.update({_id:1, a:1}, {$set : {a:1, b:3}});
    replTest.awaitReplication();
    x = mfoo.foo.find().next();
    assert(x._id == 1);
    assert(x.a == 1);
    assert(x.b == 3);
    x = sfoo.foo.find().next();
    assert(x._id == 1);
    assert(x.a == 1);
    assert(x.b == 3);
    // cases where pk changes handled in doNormalupdates    
    mfoo.foo.drop();


    replTest.stopSet(signal);
}
doTest(15, 31000, 1000000);

