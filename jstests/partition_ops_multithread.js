t = db.part_ops;
tname = "part_ops";

// test if one thread has transactionally
// added or dropped a partition, no other
// thread can simultaneously do a fileop
t.drop();

assert.commandWorked(db.runCommand({ create: tname, partitioned:1}));
assert.commandWorked(db.runCommand({addPartition:tname, newMax:{_id:10} }));
assert.commandWorked(db.runCommand({addPartition:tname, newMax:{_id:20} }));
assert.commandWorked(db.runCommand({addPartition:tname, newMax:{_id:30} }));
assert.commandWorked(db.runCommand({addPartition:tname, newMax:{_id:40} }));

// now we have five partitions
db.beginTransaction();
assert.commandWorked(db.runCommand({dropPartition:tname, id:0}));
s = startParallelShell(' \
    t = db.part_ops; \
    tname = "part_ops"; \
    assert.commandFailed(db.runCommand({addPartition:tname})); \
    assert.commandFailed(db.runCommand({addPartition:tname, newMax:{_id:100}})); \
    assert.commandFailed(db.runCommand({dropPartition:tname, id:3})); \
');

s();

sleep(2000);

db.rollbackTransaction();

db.beginTransaction();
assert.commandWorked(db.runCommand({addPartition:tname, newMax:{_id:100}}));
s = startParallelShell(' \
    t = db.part_ops; \
    tname = "part_ops"; \
    assert.commandFailed(db.runCommand({addPartition:tname})); \
    assert.commandFailed(db.runCommand({addPartition:tname, newMax:{_id:200}})); \
    assert.commandFailed(db.runCommand({dropPartition:tname, id:3})); \
');

