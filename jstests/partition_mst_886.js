t = db.part_ops;
tname = "part_ops";

// test 886
t.drop();

assert.commandWorked(db.runCommand({ create: tname, partitioned:1}));
for (i = 0; i < 200; i++) {
    t.insert({_id:i, a:i});
}

// transactionally delete 100 through 199
// but don't commit
db.beginTransaction();
for (i = 100; i < 200; i++) {
    t.remove({_id:i});
}
s = startParallelShell(' \
    t = db.part_ops; \
    tname = "part_ops"; \
    assert.commandFailed(db.runCommand({addPartition:tname, newPivot:{_id:199}})); \
    assert.commandWorked(db.runCommand({addPartition:tname})); \
');

s();

sleep(2000);

db.rollbackTransaction();

partInfo = db.runCommand({getPartitionInfo:"part_ops"});
assert.eq(2, partInfo.numPartitions);
// make sure we have the right pivot
assert.eq(199, partInfo["partitions"][0]["max"]["_id"]);

for (i = 0; i < 200; i++) {
    x = t.find({_id:i});
    assert(x.hasNext());
    assert.eq(i, x.next().a);
}
t.drop();
