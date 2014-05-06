// Test that secondary indexes on partitioned collections work
// Note that the more complicated task of making sure cursors work
// is NOT covered by this test
tn = "part_sec_key";
t = db[tn];
t.drop();


function createData() {
    assert.commandWorked(db.createCollection(tn, {partitioned:1}));
    assert.commandWorked(t.addPartition({_id:10}));
    assert.commandWorked(t.addPartition({_id:20}));
    x = t.getPartitionInfo();
    assert.eq(3, x.numPartitions);
    // insert some data
    for (i = 1; i <= 10; i++) {
        t.insert({ _id : i , a : i, b : i});
        t.insert({ _id : 10+i , a : i});
        t.insert({ _id : 20+i , a : i, b : i});
    }
}

function simpleAddIndexTest() {
    // test that a normal add index works
    t.ensureIndex({a:1})
    e = db.getLastErrorObj();
    assert(t.getIndexes().length == 2);
    assert(e.err == null);
    // now do a query
    x = t.find({ a : {$gte : 5}}).explain();
    assert(x.cursor == "DistributedPartitionedCursor");
    t.find({ a : {$gte : 5}}).forEach( function(x) { 
        //printjson(x); 
        assert.eq( x["_id"] % 10 , x["a"] % 10);
        });
}

function simpleDropIndexTest(remainingIndexes) {
    assert.commandWorked(t.dropIndex({a:1}));
    assert(t.getIndexes().length == remainingIndexes);
    assert.eq(30, t.count());
}

// simple add index test
createData();
simpleAddIndexTest();
t.drop();

// simple drop index test
createData();
simpleAddIndexTest();
simpleDropIndexTest(1);
t.drop();

// verify opening a partitioned collection with multiple indexes works
// also shows transactionally adding an index works
createData();
db.beginTransaction();
simpleAddIndexTest();
db.rollbackTransaction();
assert(t.getIndexes().length == 1);
simpleAddIndexTest();
db.beginTransaction();
t.ensureIndex({b:1})
e = db.getLastErrorObj();
assert(t.getIndexes().length == 3);
assert(e.err == null);
db.rollbackTransaction(); // this should cause collection to close
assert(t.getIndexes().length == 2);
t.drop();

// transactionally dropping an index works
createData();
simpleAddIndexTest();
t.ensureIndex({b:1})
e = db.getLastErrorObj();
assert(t.getIndexes().length == 3);
db.beginTransaction();
simpleDropIndexTest(2);
db.rollbackTransaction();
assert(t.getIndexes().length == 3);
t.drop();

// test that adding and dropping a partition works
createData();
simpleAddIndexTest();
assert.commandWorked(t.addPartition());
for (i = 31; i <= 40; i++) {
    t.insert({ _id : i , a : i, b : i});
}
x = t.getPartitionInfo();
//printjson(x);
assert(x.numPartitions == 4);
assert(x["partitions"][3]["_id"] == 3);
assert.commandWorked(t.dropPartition(3)); // should be dropping last partition
assert(t.count({_id : {$gt : 30}}) == 0); // verify data dropped
// now add another partition, which should reuse the same id
assert.commandWorked(t.addPartition());
x = t.getPartitionInfo();
//printjson(x);
assert(x.numPartitions == 4);
assert(x["partitions"][3]["_id"] == 3);
t.drop();

// show that a unique index and a clustering index will work
createData();
t.ensureIndex({a:1}, {unique : true})// works, even though a is not truly unique, because it is unique per partition
e = db.getLastErrorObj();
assert(t.getIndexes().length == 2);
assert(e.err == null);
t.update({_id:15},{$set : {a : 1}}); // this should make adding a unique index fail
e = db.getLastErrorObj();
assert(e.err != null);
simpleDropIndexTest(1);
t.update({_id:15},{$set : {a : 1}}); // this should make adding a unique index fail
e = db.getLastErrorObj();
// now adding the unique index should fail
t.ensureIndex({a:1}, {unique : true})
e = db.getLastErrorObj();
assert(t.getIndexes().length == 1);
assert(e.err != null);
t.drop();

