// test that dump/restore of a partitioned collection works as expected

doTest = function (dbName, recreateBeforeRestore) {
    t = new ToolTest( "dumprestore_pc" );
    t.startDB( dbName );

    db = t.db;

    db.createCollection("foo", {partitioned:1});
    db.foo.addPartition({_id:100});
    db.foo.addPartition({_id:200});
    db.foo.addPartition({_id:300});
    db.foo.dropPartition(0);
    for (var i = 0; i < 400; i++) {
        db.foo.insert({_id : i});
    }
    expectedCount = 400;

    assert(db.foo.count() == expectedCount);
    x = db.foo.getPartitionInfo();

    t.runTool( "dump" , "--out" , t.ext );

    db.foo.drop();
    assert.eq( 0, db.foo.count() , "after drop" );
    if (recreateBeforeRestore) {
        db.createCollection("foo", {partitioned:1});
        for (var i = 0; i < 10; i++) {
            db.foo.insert({_id : 1000+i});
        }
        expectedCount = 410;
        x = db.foo.getPartitionInfo();
    }

    t.runTool( "restore" , "--dir" , t.ext );
    assert.eq(db.foo.count(), expectedCount);
    y = db.foo.getPartitionInfo();
    // I cannot do a "friendlyEqual" because the dump/restore gets rid of the NumberLong() around
    // the partition's id field
    printjson(x);
    printjson(y);
    assert.eq(x.numPartitions, y.numPartitions);
    for (var i = 0; i < x.numPartitions; i++) {
        assert.eq(x["partitions"][i]["_id"], y["partitions"][i]["_id"]);
        assert(friendlyEqual(x["partitions"][i]["max"], x["partitions"][i]["max"]));
        assert.eq(x["partitions"][i]["createTime"], y["partitions"][i]["createTime"]);
    }
    t.stop();
}

doTest("foo", false);
doTest("fooff", true);
