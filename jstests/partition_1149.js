t = db.part_1149;
tname = "part_1149";

// test if a partition is added while a transaction is live,
// then that transaction will not be able to read data in the new partition
t.drop();

assert.commandWorked(db.runCommand({ create: tname, partitioned:1}));
assert.commandWorked(db.runCommand({addPartition:tname, newMax:{_id:10} }));
t.insert([ {_id:1} , {_id:11} ]);
assert.eq(null, db.getLastError());

function checkData() {
    assert.eq(t.count(), 2);
    x = t.find();
    assert(x.hasNext());
    y = x.next();
    assert.eq(y._id, 1);
    y = x.next();
    assert.eq(y._id, 11);
    assert(!x.hasNext());
}

db.beginTransaction();
checkData();

s = startParallelShell(' \
    t = db.part_1149; \
    tname = "part_1149"; \
    assert.commandWorked(t.addPartition({_id:20})); \
    t.insert({_id:21}); \
    assert.eq(t.count(), 3); \
');

s();

sleep(2000);

checkData();

db.rollbackTransaction();

t.drop();
