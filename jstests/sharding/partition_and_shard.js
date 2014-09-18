// Tests partitioned and sharded collections

doTest = function(hashKey) {
    var st = new ShardingTest( { shards : 2, mongos : 1, separateConfig : 1  } );
    // Stop balancer, it'll interfere
    st.stopBalancer();

    var admin = st.s.getDB( "admin" );
    var db = st.s.getDB("foo");
    var configs = st._configServers[0];
    assert.commandWorked(admin.runCommand({enableSharding : "foo"}));

    // create a partitioned collection, but not sharded
    db.createCollection("foo", {partitioned : true});
    assert.commandWorked(db.foo.getPartitionInfo());

    // try sharding an existing partitioned collection and fail
    print("testing create");
    assert.commandFailed(admin.runCommand({shardCollection : "foo.foo", key : {a : 1}}));
    db.foo.drop();

    // fails because partitionKey must be a valid PK
    assert.commandFailed(admin.runCommand({shardCollection : "foo.foo", key : {a : 1}, partitionKey : {ts : 1}}));
    if (hashKey) {
        assert.commandWorked(admin.runCommand({shardCollection : "foo.foo", key : {_id : "hashed"}, partitionKey : {ts : 1, _id : 1}}));
    }
    else {
        assert.commandWorked(admin.runCommand({shardCollection : "foo.foo", key : {a : 1}, partitionKey : {ts : 1, _id : 1}}));
    }
    assert.commandWorked(admin.runCommand({shardCollection : "foo.bar", key : {a : 1}}));

    // verify that we see foo.foo in the config and we can get a partition info
    x = configs.getDB("config").collections.find({ _id : "foo.foo"}).next();
    assert.eq(false, x.dropped);

    assert.commandWorked(db.foo.getPartitionInfo());
    x = db.foo.getPartitionInfo();
    printjson(x);

    // at this point, we know we have created a partitioned collection and that it is sharded
    // it only exists on the first shard, at the moment
    // now let's add some partitions
    print("adding partitions");
    assert.commandWorked(db.foo.addPartition({ts : 10, _id: MaxKey}));
    assert.commandWorked(db.foo.addPartition({ts : 20, _id: MaxKey}));
    assert.commandWorked(db.foo.addPartition({ts : 30, _id: MaxKey}));
    assert.commandWorked(db.foo.addPartition({ts : 40, _id: MaxKey}));
    x = db.foo.getPartitionInfo();
    printjson(x);

    // now let's connect to the primary shard and verify that it has 5 partitions
    var primary = st.getServer("foo");
    var nonPrimary = st.getOther(primary);
    x = primary.getDB("foo").foo.getPartitionInfo();
    printjson(x);
    assert.eq(x.numPartitions, 5);

    // now let's do some insertions
    print("making sure can't insert arrays");
    if (!hashKey) {
        db.foo.insert({a : [5, 6], ts : 3});
        assert(db.getLastError() != null);
    }
    db.foo.insert({ts : [5, 6], a : 3});
    assert(db.getLastError() != null);
    print("inserting data");
    for (var i = 1; i <= 50; i++) {
        x = {_id : i, ts: i, a : 2*i, b : Random.rand()}
        db.foo.insert(x);
        db.bar.insert(x);
    }
    assert.eq( null, db.getLastError() );
    assert.eq(db.foo.count(), 50);

    // now verify that the indexes are right
    print("verifying indexes");
    x = primary.getDB("foo").foo.getIndexes();
    assert.eq(x.length, 3); // _id, pk, shard key
    assert.eq("primaryKey", x[0]["name"]);
    assert.eq("_id_", x[1]["name"]);
    if (hashKey) {
        assert.eq("_id_hashed", x[2]["name"]);
    }
    else {
        assert.eq("a_1", x[2]["name"]);
    }
    // verify indexes are right
    assert(friendlyEqual(x[0]["key"], {ts : 1, _id : 1}));
    assert(friendlyEqual(x[1]["key"], {_id : 1}));
    if (hashKey) {
        assert(friendlyEqual(x[2]["key"], {_id : "hashed"}));
    }
    else {
        assert(friendlyEqual(x[2]["key"], {a : 1}));
    }
    // verify you cannot drop any of these indexes
    assert.commandFailed(db.foo.dropIndex({_id : 1}));
    assert.commandFailed(db.foo.dropIndex({ts : 1, _id : 1}));
    // turns out if the shard key is not the PK, it can be dropped
    //assert.commandFailed(db.foo.dropIndex({a : 1}));

    // now test that a split and migrate work
    if (!hashKey) {
        print("testing split and migrate");
        assert.commandWorked(admin.runCommand({split : "foo.foo", middle : {a : 62}}));
        //assert.commandWorked(sh.splitAt("foo.foo", {a : 62}));
        assert.commandWorked(admin.runCommand({moveChunk : "foo.foo", find : {a : 62}, to : nonPrimary.name}));
        assert.eq(30, primary.getDB("foo").foo.count());
        assert.eq(20, nonPrimary.getDB("foo").foo.count());
    }

    x = nonPrimary.getDB("foo").foo.getPartitionInfo();
    y = primary.getDB("foo").foo.getPartitionInfo();
    // make sure that the partitions are the same on both shards, migrate should take care of this
    if (!hashKey) {
        assert(friendlyEqual(x,y));
    }
    else {
        // let's just make sure the number of partitions is the same
        assert.eq(x.numPartitions, y.numPartitions);
    }
    // make sure indexes are the same on this second shard
    x = nonPrimary.getDB("foo").foo.getIndexes();
    y = primary.getDB("foo").foo.getIndexes();
    assert(friendlyEqual(x,y));

    // now let's create an index and do some queries
    print("adding index");
    db.foo.ensureIndex({b:1});
    assert(!db.getLastError());
    db.bar.ensureIndex({b:1});
    assert(!db.getLastError());
    // make sure that each shard for foo has
    // the index
    x = primary.getDB("foo").foo.getIndexes();
    assert.eq(x.length, 4); // _id, pk, shard key, b
    assert.eq("primaryKey", x[0]["name"]);
    assert.eq("_id_", x[1]["name"]);
    if (hashKey) {
        assert.eq("_id_hashed", x[2]["name"]);
    }
    else {
        assert.eq("a_1", x[2]["name"]);
    }
    assert.eq("b_1", x[3]["name"]);
    assert(friendlyEqual(x[0]["key"], {ts : 1, _id : 1}));
    assert(friendlyEqual(x[1]["key"], {_id : 1}));
    if (hashKey) {
        assert(friendlyEqual(x[2]["key"], {_id : "hashed"}));
    }
    else {
        assert(friendlyEqual(x[2]["key"], {a : 1}));
    }
    assert(friendlyEqual(x[3]["key"], {b : 1}));
    y = nonPrimary.getDB("foo").foo.getIndexes();
    assert(friendlyEqual(x,y));

    // queries
    print("running queries");
    for (var qi = 0; qi < 20; qi++) {
        print("query iteration " + i);
        query = { b: {$gte:Random.rand()}};
        x = db.foo.find(query).sort({b:1}).limit(5);
        y = db.bar.find(query).sort({b:1}).limit(5);
        for (var i = 0; i < 5; i++) {
            if (x.hasNext()) {
                assert(y.hasNext());
                xVal = x.next();
                yVal = y.next();
                printjson(xVal);
                printjson(yVal);
                assert.eq(x._id, y._id);
                assert.eq(x.ts, y.ts);
                assert.eq(x.a, y.a);
                assert.eq(x.b, y.b);
            }
        }
        assert(!x.hasNext());
        assert(!y.hasNext());

        query = { b: {$lte:Random.rand()}};
        x = db.foo.find(query).sort({b:-1}).limit(5);
        y = db.bar.find(query).sort({b:-1}).limit(5);
        for (var i = 0; i < 5; i++) {
            if (x.hasNext()) {
                assert(y.hasNext());
                xVal = x.next();
                yVal = y.next();
                printjson(xVal);
                printjson(yVal);
                assert.eq(x._id, y._id);
                assert.eq(x.ts, y.ts);
                assert.eq(x.a, y.a);
                assert.eq(x.b, y.b);
            }
        }
        assert(!x.hasNext());
        assert(!y.hasNext());
    }

    // now let's drop some partitions
    assert.commandWorked(db.foo.dropPartitionsLEQ({ts : 20, _id : MaxKey}));
    // this should make it so that we now have 3 partitions
    x = primary.getDB("foo").foo.getPartitionInfo();
    printjson(x);
    assert.eq(x.numPartitions, 3);
    if (!hashKey) {
        assert.eq(10, primary.getDB("foo").foo.count());
        assert.eq(20, nonPrimary.getDB("foo").foo.count());
        assert.eq(30, db.foo.count());
        x = db.foo.find().sort({ts : 1}).next();
        assert.eq(x.ts, 21);
    }
    x = nonPrimary.getDB("foo").foo.getPartitionInfo();
    y = primary.getDB("foo").foo.getPartitionInfo();
    // make sure that the partitions are the same on both shards, migrate should take care of this
    if (!hashKey) {
        assert(friendlyEqual(x,y));
    }
    else {
        assert.eq(x.numPartitions, y.numPartitions);
    }

    // now let's do an add partition test
    assert.commandWorked(db.foo.addPartition({ts : 50, _id : MaxKey}));
    x = nonPrimary.getDB("foo").foo.getPartitionInfo();
    assert.eq(x.numPartitions, 4);
    y = primary.getDB("foo").foo.getPartitionInfo();
    assert.eq(y.numPartitions, 4);
    assert(friendlyEqual(x["partitions"][3]["max"], y["partitions"][3]["max"]));

    // now let's ensure that dropping the collection works
    assert(db.foo.drop());
    // make sure the collection does not exist on either shard
    assert.commandFailed(primary.getDB("foo").runCommand({'_collectionsExist': ['foo.foo']}));
    assert.commandFailed(nonPrimary.getDB("foo").runCommand({'_collectionsExist': ['foo.foo']}));

    st.stop();
};

var repeatStr = function(str, n){
  return new Array(n + 1).join(str);
};

// this is a crude test that autosplit doesn't happen,
// we simply insert a bunch of rows, and verify that splitting doesn't
// occur by ensuring that only one chunk exists
doAutosplitTest = function() {
    var st = new ShardingTest( { shards : 2, mongos : 1, separateConfig : 1  } );
    // Stop balancer, it'll interfere
    st.stopBalancer();

    var admin = st.s.getDB( "admin" );
    var db = st.s.getDB("foo");
    var configs = st._configServers[0];
    assert.eq(configs.getDB("config").chunks.count(), 0);

    assert.commandWorked(admin.runCommand({enableSharding : "foo"}));
    assert.commandWorked(admin.runCommand({shardCollection : "foo.foo", key : {a : 1}, partitionKey : {ts : 1, _id : 1}}));
    assert.eq(configs.getDB("config").chunks.count(), 1);
    // 10M insertions, should be at least 64MB of data
    // will verify that there is still just one chunk at the end
    print("inserting docs");
    for (var i = 1; i <= 1000; i++) {
        if (i%50 == 0) {
            print("inserted " + i + " docs");
        }
        x = {_id : i, ts: i, a : 2*i, b : repeatStr('c', 100000)}
        db.foo.insert(x);
    }
    assert.eq(configs.getDB("config").chunks.count(), 1);

    st.stop();
}

doAutosplitTest();
//doTest(true);
//doTest(false);

