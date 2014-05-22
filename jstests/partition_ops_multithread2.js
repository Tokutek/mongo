t = db.part_ops;
tname = "part_ops";


t.drop();

assert.commandWorked(db.runCommand({ create: tname, partitioned:1}));
assert.commandWorked(db.runCommand({addPartition:tname, newMax:{_id:10} }));
assert.commandWorked(db.runCommand({addPartition:tname, newMax:{_id:20} }));
assert.commandWorked(db.runCommand({addPartition:tname, newMax:{_id:30} }));
assert.commandWorked(db.runCommand({addPartition:tname, newMax:{_id:40} }));

// now we have five partitions

// test that if a partition gets dropped while a snapshot transaction is live,
// that if the snapshot transaction later tries to drop that same partition,
// it will fail.
// this is testing #1148. Before #1148, this crashed the server on debug builds

db.beginTransaction();
s = startParallelShell(' \
    t = db.part_ops; \
    tname = "part_ops"; \
    assert.commandWorked(t.dropPartition(4)); \
');

s();

sleep(2000);

x = t.getPartitionInfo();
assert.eq(x.numPartitions, 5);
assert.eq(x["partitions"][4]["_id"], 4);
assert.commandFailed(t.dropPartition(4));

db.rollbackTransaction();

// do a similar test with addPartition

db.beginTransaction();
s = startParallelShell(' \
    t = db.part_ops; \
    tname = "part_ops"; \
    assert.commandWorked(t.addPartition({_id : 100})); \
');

s();
sleep(2000);
x = t.getPartitionInfo();
assert.eq(x.numPartitions, 4);
assert.commandFailed(t.addPartition({_id : 50}));
db.rollbackTransaction();
