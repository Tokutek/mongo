// Despite the bad title, this unit tests partition add AND partition drop.

tname = "part_add";
t = db.part_add;
tt = db.not_part;
t.drop();
tt.drop();

tt.insert({a:1}); // make a non-partitioned collection to verify that partition operations fail on it

assert.commandWorked(db.runCommand({ create: tname, partitioned:1}));
assert.commandFailed(db.runCommand({getPartitionInfo: "not_part"})); // not a partitioned collection

x = db.runCommand({getPartitionInfo:tname});
assert.eq(1, x.numPartitions);
assert.eq(0, x.partitions[0]._id);
assert.eq(MaxKey,  x["partitions"][0]["max"][""]);
// test dropping only partition fails
assert.commandFailed(db.runCommand({dropPartition:tname, id:0}));

for (i = 0; i < 20; i += 2) {
    t.insert({_id:i});
}
assert.commandWorked(db.runCommand({addPartition:tname}));
x = db.runCommand({getPartitionInfo:tname});
assert.eq(2, x.numPartitions);
assert.eq(0, x.partitions[0]._id);
assert.eq(1, x.partitions[1]._id);
// a pivot of 18 should have been created, the max
// element in the last partition
assert.eq(18,  x["partitions"][0]["max"]["_id"]);
assert.eq(MaxKey,  x["partitions"][1]["max"][""]);

// drop and readd the partition
assert.commandWorked(db.runCommand({dropPartition:tname, id:1}));
x = db.runCommand({getPartitionInfo:tname});
assert.eq(1, x.numPartitions);
assert.eq(0, x.partitions[0]._id);
assert.eq(MaxKey,  x["partitions"][0]["max"][""]);
assert.commandWorked(db.runCommand({addPartition:tname}));
x = db.runCommand({getPartitionInfo:tname});
assert.eq(2, x.numPartitions);
assert.eq(0, x.partitions[0]._id);
assert.eq(1, x.partitions[1]._id);
// a pivot of 18 should have been created, the max
// element in the last partition
assert.eq(18,  x["partitions"][0]["max"]["_id"]);
assert.eq(MaxKey,  x["partitions"][1]["max"][""]);

// too low
assert.commandFailed(db.runCommand({addPartition:tname, newPivot:{_id:12} }));
assert.commandFailed(db.runCommand({addPartition:tname, newPivot:{_id:18}}));
// no data in last partition
assert.commandFailed(db.runCommand({addPartition:tname}));
assert.commandWorked(db.runCommand({addPartition:tname, newPivot:{_id:24}}));

// now let's insert more data
for (i = 20; i < 40; i+=2) {
    t.insert({_id:i});
}
assert.commandFailed(db.runCommand({addPartition:tname, newPivot:{_id:18}}));
assert.commandFailed(db.runCommand({addPartition:tname, newPivot:{_id:38}}));
assert.commandWorked(db.runCommand({addPartition:tname, newPivot:{_id:39}}));

x = db.runCommand({getPartitionInfo:tname});
assert.eq(4, x.numPartitions);
assert.eq(0, x.partitions[0]._id);
assert.eq(1, x.partitions[1]._id);
assert.eq(2, x.partitions[2]._id);
assert.eq(3, x.partitions[3]._id);
assert.eq(18,  x["partitions"][0]["max"]["_id"]);
assert.eq(24,  x["partitions"][1]["max"]["_id"]);
assert.eq(39,  x["partitions"][2]["max"]["_id"]);
assert.eq(MaxKey,  x["partitions"][3]["max"][""]);

// try an add and drop partition in an MST
assert.commandWorked(db.beginTransaction());
assert.commandFailed(db.runCommand({addPartition:tname, newPivot:{_id:39}}));
assert.commandWorked(db.runCommand({addPartition:tname, newPivot:{_id:40}}));
assert.eq(20, t.count());
assert.commandWorked(db.runCommand({dropPartition:tname, id:0}));
assert.eq(10, t.count());

x = db.runCommand({getPartitionInfo:tname});
assert.eq(4, x.numPartitions);
assert.eq(1, x.partitions[0]._id);
assert.eq(2, x.partitions[1]._id);
assert.eq(3, x.partitions[2]._id);
assert.eq(4, x.partitions[3]._id);
assert.eq(24,  x["partitions"][0]["max"]["_id"]);
assert.eq(39,  x["partitions"][1]["max"]["_id"]);
assert.eq(40,  x["partitions"][2]["max"]["_id"]);
assert.eq(MaxKey,  x["partitions"][3]["max"][""]);
assert.commandWorked(db.rollbackTransaction());

// make sure it successfully rolled back
assert.eq(20, t.count());
x = db.runCommand({getPartitionInfo:tname});
assert.eq(4, x.numPartitions);
assert.eq(0, x.partitions[0]._id);
assert.eq(1, x.partitions[1]._id);
assert.eq(2, x.partitions[2]._id);
assert.eq(3, x.partitions[3]._id);
assert.eq(18,  x["partitions"][0]["max"]["_id"]);
assert.eq(24,  x["partitions"][1]["max"]["_id"]);
assert.eq(39,  x["partitions"][2]["max"]["_id"]);
assert.eq(MaxKey,  x["partitions"][3]["max"][""]);

// try just an add, and do a simple test that
// things still work after the rollback
assert.commandWorked(db.beginTransaction());
assert.commandWorked(db.runCommand({addPartition:tname, newPivot:{_id:40}}));
x = db.runCommand({getPartitionInfo:tname});
assert.eq(5, x.numPartitions);
assert.commandWorked(db.rollbackTransaction());
x = db.runCommand({getPartitionInfo:tname});
assert.eq(4, x.numPartitions);

// try just a drop
assert.commandWorked(db.beginTransaction());
assert.commandWorked(db.runCommand({dropPartition:tname, id:0}));
x = db.runCommand({getPartitionInfo:tname});
assert.eq(3, x.numPartitions);
assert.commandWorked(db.rollbackTransaction());
x = db.runCommand({getPartitionInfo:tname});
assert.eq(4, x.numPartitions);

// now test drop partition for real.
// we have even numbers from 0 to 40 inserted
// we have partitions with pivots of 18, 24, 39, and MaxKey
// let's add a 5th partition while we are at it
assert.commandWorked(db.runCommand({addPartition:tname, newPivot:{_id:60}}));
// let's insert some values
for (i = 40; i < 80; i+=2) {
    t.insert({_id:i});
}

// now we have even nubers from 0 to 80 inserted, with pivots of 18, 24, 39, 60, and MaxKey
// first verify partition ids
x = db.runCommand({getPartitionInfo:tname});
assert.eq(5, x.numPartitions);
assert.eq(0, x.partitions[0]._id);
assert.eq(1, x.partitions[1]._id);
assert.eq(2, x.partitions[2]._id);
assert.eq(3, x.partitions[3]._id);
assert.eq(4, x.partitions[4]._id);

// test dropping the first partition
y = t.count({_id: {$lte: 18}});
assert.eq(y, 10);
assert.commandWorked(db.runCommand({dropPartition:tname, id:0}));
// let's verify that we have 4 partitions, and no data from 0 to 18
x = db.runCommand({getPartitionInfo:tname});
assert.eq(4, x.numPartitions);
assert.eq(1, x.partitions[0]._id);
assert.eq(30, t.count());
y = t.count({_id: {$lte: 18}});
assert.eq(y, 0);

// now try dropping the last partition
y = t.count({_id: {$gt: 60}});
assert.eq(y, 9);
assert.commandWorked(db.runCommand({dropPartition:tname, id:4}));
x = db.runCommand({getPartitionInfo:tname});
assert.eq(3, x.numPartitions);
assert.eq(1, x.partitions[0]._id);
assert.eq(2, x.partitions[1]._id);
assert.eq(3, x.partitions[2]._id);
assert.eq(24,  x["partitions"][0]["max"]["_id"]);
assert.eq(39,  x["partitions"][1]["max"]["_id"]);
assert.eq(MaxKey,  x["partitions"][2]["max"][""]); // verify the last pivot has changed
// verify data dropped
y = t.count({_id: {$gt: 60}});
assert.eq(y, 0);

// test dropping the middle partition, getting rid of pivot 39
y = t.count({_id: {$gt: 24, $lte:39}});
assert.eq(y, 7);
assert.commandWorked(db.runCommand({dropPartition:tname, id:2}));
x = db.runCommand({getPartitionInfo:tname});
assert.eq(2, x.numPartitions);
assert.eq(1, x.partitions[0]._id);
assert.eq(3, x.partitions[1]._id);
assert.eq(24,  x["partitions"][0]["max"]["_id"]);
assert.eq(MaxKey,  x["partitions"][1]["max"][""]); // verify the last pivot has changed
y = t.count({_id: {$gt: 24, $lte:39}});
assert.eq(y, 0);

assert.eq(t.count(), 14);

// make sure bad partition id returns error
assert.commandFailed(db.runCommand({dropPartition:tname, id:"asdf"}));
assert.commandFailed(db.runCommand({dropPartition:tname, id:"34"}));

t.drop();
tt.drop();

