// Test that simple operations on a partitioned collection work
// This verifies that a partitioned collection with just a single partition
// works as expected

t = db.part_convert;
tname = 'part_convert';
t.drop();

// test that we cannot run it on a non-existent collection
assert.commandFailed(db.runCommand({convertToPartitioned:tname}));
// test that we cannot convert a collection with a pk
assert.commandWorked(db.runCommand({ create: tname, primaryKey: { a: 1, _id: 1 } }));
assert.commandFailed(db.runCommand({convertToPartitioned:tname}));
t.drop();
// test that we cannot convert a capped collection
assert.commandWorked(db.runCommand({ create: tname, capped:1, size:1000000}));
assert.commandFailed(db.runCommand({convertToPartitioned:tname}));
t.drop();
// test that we cannot convert a collection that is already partitioned
assert.commandWorked(db.runCommand({ create: tname, partitioned:1}));
assert.commandFailed(db.runCommand({convertToPartitioned:tname}));
t.drop();
// test that we cannot convert a collection with indexes
assert.commandWorked(db.runCommand({ create: tname}));
t.ensureIndex({a:1});
db.getLastError();
assert.commandFailed(db.runCommand({convertToPartitioned:tname}));
t.drop();

// test that we can convert an empty collection to partitioned
// and do a couple of simple operations
assert.commandWorked(db.runCommand({ create: tname}));
assert.commandWorked(db.runCommand({convertToPartitioned:tname}));
x = db.runCommand({getPartitionInfo:tname});
assert.eq(x.numPartitions, 1);
assert.eq(x["partitions"][0]["_id"], 0);
assert.eq(MaxKey,  x["partitions"][0]["max"]["_id"]);
t.insert({_id:0});
t.insert({_id:1});
assert.eq(1, t.count({_id:0}));
assert.eq(1, t.count({_id:1}));
assert.commandWorked(db.runCommand({addPartition:tname}));
x = db.runCommand({getPartitionInfo:tname});
assert.eq(x.numPartitions, 2);
assert.eq(x["partitions"][0]["_id"], 0);
assert.eq(x["partitions"][1]["_id"], 1);
assert.eq(1,  x["partitions"][0]["max"]["_id"]);
assert.eq(MaxKey,  x["partitions"][1]["max"]["_id"]);
t.insert({_id:2});
t.insert({_id:3});
assert.eq(1, t.count({_id:2}));
assert.eq(1, t.count({_id:3}));
assert.commandWorked(db.runCommand({dropPartition:tname, id:0}));
assert.eq(0, t.count({_id:0}));
assert.eq(0, t.count({_id:1}));
assert.eq(1, t.count({_id:2}));
assert.eq(1, t.count({_id:3}));
t.drop();

// test that we can convert a non-empty collection to partitioned
// and do some simple operations
assert.commandWorked(db.runCommand({ create: tname}));
t.insert({_id:0});
t.insert({_id:1});
assert.commandWorked(db.runCommand({convertToPartitioned:tname}));
x = db.runCommand({getPartitionInfo:tname});
assert.eq(x.numPartitions, 1);
assert.eq(x["partitions"][0]["_id"], 0);
assert.eq(MaxKey,  x["partitions"][0]["max"]["_id"]);
assert.eq(1, t.count({_id:0}));
assert.eq(1, t.count({_id:1}));
assert.commandWorked(db.runCommand({addPartition:tname}));
x = db.runCommand({getPartitionInfo:tname});
assert.eq(x.numPartitions, 2);
assert.eq(x["partitions"][0]["_id"], 0);
assert.eq(x["partitions"][1]["_id"], 1);
assert.eq(1,  x["partitions"][0]["max"]["_id"]);
assert.eq(MaxKey,  x["partitions"][1]["max"]["_id"]);
t.insert({_id:2});
t.insert({_id:3});
assert.eq(1, t.count({_id:2}));
assert.eq(1, t.count({_id:3}));
assert.commandWorked(db.runCommand({dropPartition:tname, id:0}));
assert.eq(0, t.count({_id:0}));
assert.eq(0, t.count({_id:1}));
assert.eq(1, t.count({_id:2}));
assert.eq(1, t.count({_id:3}));
t.drop();

// do an mst test
assert.commandWorked(db.runCommand({ create: tname}));
t.insert({_id:0});
t.insert({_id:1});
db.beginTransaction();
assert.commandWorked(db.runCommand({convertToPartitioned:tname}));
x = db.runCommand({getPartitionInfo:tname});
assert.eq(x.numPartitions, 1);
assert.eq(x["partitions"][0]["_id"], 0);
assert.eq(MaxKey,  x["partitions"][0]["max"]["_id"]);
assert.eq(1, t.count({_id:0}));
assert.eq(1, t.count({_id:1}));
assert.commandWorked(db.runCommand({addPartition:tname}));
x = db.runCommand({getPartitionInfo:tname});
assert.eq(x.numPartitions, 2);
assert.eq(x["partitions"][0]["_id"], 0);
assert.eq(x["partitions"][1]["_id"], 1);
assert.eq(1,  x["partitions"][0]["max"]["_id"]);
assert.eq(MaxKey,  x["partitions"][1]["max"]["_id"]);
t.insert({_id:2});
t.insert({_id:3});
assert.eq(1, t.count({_id:2}));
assert.eq(1, t.count({_id:3}));
assert.commandWorked(db.runCommand({dropPartition:tname, id:0}));
assert.eq(0, t.count({_id:0}));
assert.eq(0, t.count({_id:1}));
assert.eq(1, t.count({_id:2}));
assert.eq(1, t.count({_id:3}));
db.rollbackTransaction();
assert.commandFailed(db.runCommand({getPartitionInfo:tname}));
assert.eq(1, t.count({_id:0}));
assert.eq(1, t.count({_id:1}));
t.drop();

