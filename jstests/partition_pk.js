// test that we can have a partitioned collection with a PK
tn = "partition_pk";
t = db[tn];
t.drop();

assert.commandWorked(db.createCollection(tn, {partitioned:1, primaryKey : {a:1, _id:1 }}));
assert(t.getIndexes().length == 2); // make sure both indexes were created
// make sure a simple drop/recreate works
t.drop();
assert.commandWorked(db.createCollection(tn, {partitioned:1, primaryKey : {a:1, _id:1 }}));
assert(t.getIndexes().length == 2); // make sure both indexes were created


fullName = db.getName() + '.' + tn;
// verify system.namespaces has the right information
assert(db.system.namespaces.count({name:fullName + '.$primaryKey'}) == 1);
assert(db.system.namespaces.count({name:fullName + '.$_id_'}) == 1);
x = db.system.namespaces.find({name : fullName});
assert(x.hasNext());
y = x.next();
assert(y["options"]["partitioned"] == 1);

// make sure system.indexes has the right stuff
assert(db.system.indexes.count({ns : fullName}) == 2);

// now let's insert some data
t.insert({a:2, _id:1});
t.insert({a:1, _id:2});

function checkData() {
    // check the data
    x = t.find();
    y = x.next();
    assert(y["a"] == 1);
    assert(y["_id"] == 2);
    y = x.next();
    assert(y["a"] == 2);
    assert(y["_id"] == 1);
    assert(!x.hasNext());
    // make sure a simple lookup by _id works
    assert(t.find({_id:1}).hasNext());
    assert(t.find({_id:2}).hasNext());
}

checkData();

// now try adding an index and rolling back, to get the collection to close and reopen
db.beginTransaction();
t.ensureIndex({b:1});
e = db.getLastErrorObj();
assert(e.err == null);
db.rollbackTransaction();

checkData(); // this data check will reopen the collection

// make sure passing a bad pivot does not work
assert.commandFailed(t.addPartition({a:0, _id:100}));
// add a partition
assert.commandWorked(t.addPartition());
x = t.getPartitionInfo();
assert(x.numPartitions == 2);
assert(x["partitions"][0]["max"]["a"] == 2);
assert(x["partitions"][0]["max"]["_id"] == 1);

t.insert({a:3, _id:3});
t.insert({a:0, _id:100});
assert(t.count() == 4);
// now lets drop that first partition and make sure the right data was dropped
t.dropPartition(0);
assert(t.count() == 1);
x = t.find().next();
assert(x["a"] == 3);
assert(x["_id"] == 3);

t.drop();
assert.commandWorked(db.createCollection(tn, {partitioned:1, primaryKey : {a:1, _id:1 }}));
assert(t.getIndexes().length == 2); // little test for 90777ddd
t.ensureIndex({b:1});
e = db.getLastErrorObj();
assert(e.err == null);
t.ensureIndex({c:1});
e = db.getLastErrorObj();
assert(e.err == null);
t.ensureIndex({d:1});
e = db.getLastErrorObj();
assert(e.err == null);
assert(t.getIndexes().length == 5); // little test for 90777ddd
t.drop();

// quick test that pk with a reverse direction works
assert.commandWorked(db.createCollection(tn, {partitioned:1, primaryKey : {a:-1, _id:1 }}));
x = t.getPartitionInfo();
assert.eq(MinKey, x["partitions"][0]["max"]["a"]);
assert.eq(MaxKey, x["partitions"][0]["max"]["_id"]);
t.insert({_id:0, a:3});
t.insert({_id:1, a:2});
t.insert({_id:2, a:1});
t.insert({_id:3, a:0});
assert.commandFailed(t.addPartition({a:4}));
assert.commandWorked(t.addPartition());
x = t.getPartitionInfo();
assert(x.numPartitions == 2);
assert.eq(0, x["partitions"][0]["max"]["a"]);
assert.eq(3, x["partitions"][0]["max"]["_id"]);
assert.eq(MinKey, x["partitions"][1]["max"]["a"]);
assert.eq(MaxKey, x["partitions"][1]["max"]["_id"]);
t.drop();

