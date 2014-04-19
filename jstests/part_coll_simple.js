7// Test that simple operations on a partitioned collection work
// This verifies that a partitioned collection with just a single partition
// works as expected

t = db.part_coll_simple;
t.drop();

// verify that we cannot create a partitioned collection
// with a custom PK or with a capped collection
assert.commandFailed(db.runCommand({ create: 'part_coll_simple', partitioned:1, capped:1}));
assert.commandFailed(db.runCommand({ create: 'part_coll_simple', partitioned:1, capped:1, primaryKey: { a: 1, _id: 1 } }));

assert.commandWorked(db.runCommand({ create: 'part_coll_simple', partitioned:1}));
admin = db.getMongo().getDB( "admin" );

// verify that we can drop and recreate
t.drop();
assert.commandWorked(db.runCommand({ create: 'part_coll_simple', partitioned:1}));
assert.commandWorked(t.reIndex());

function doWritesTest() {
    // insert some data
    for (i = 0; i < 1000; i++) {
        t.insert({_id:i, a:i});
    }

    // simple stuff to verify the data,
    // will trust queries more strenuously later
    assert.eq(1000, t.count());
    for (i = 0; i < 1000; i++) {
        x = t.find({_id:i}).next();
        assert.eq(x._id, i);
        assert.eq(x.a, i);
    }

    // update some data
    for (i = 0; i < 1000; i++) {
        t.update({_id:i}, {b :2*i});
    }
    assert.eq(1000, t.count());
    for (i = 0; i < 1000; i++) {
        x = t.find({_id:i}).next();
        assert.eq(x._id, i);
        assert.eq(x.b, 2*i);
    }

    for (i = 0; i < 1000; i++) {
        t.update({_id:i}, {$set : { b :3*i}});
    }
    assert.eq(1000, t.count());
    for (i = 0; i < 1000; i++) {
        x = t.find({_id:i}).next();
        assert.eq(x._id, i);
        assert.eq(x.b, 3*i);
    }

    // "fast" update some data (which is disabled)
    for (i = 0; i < 1000; i++) {
        t.update({_id:i}, {$inc : {b : 1}});
    }
    assert.eq(1000, t.count());
    for (i = 0; i < 1000; i++) {
        x = t.find({_id:i}).next();
        assert.eq(x._id, i);
        assert.eq(x.b, 3*i + 1);
    }

    // delete all data
    for (i = 0; i < 500; i++) {
        t.remove({_id:i});
    }
    assert.eq(500, t.count());

    // check the data
    for (i = 500; i < 1000; i++) {
        x = t.find({_id:i}).next();
        assert.eq(x._id, i);
        assert.eq(x.b, 3*i + 1);
    }
}
function doQueriesTest() {
    // now let's do some real query tests
    for (i = 0; i < 1000; i++) {
        t.insert({_id:i, a:2*i, b:3*i});
    }

    // count tests:
    assert.eq(1000, t.count());
    assert.eq(500, t.count({ _id: { $gte : 500 }  }));
    assert.eq(1, t.count( {a:400} )); // test with a matcher

    // findByPK tested well above

    // test forward table scan
    explain = t.find().explain();
    assert.eq(1000, explain.nscanned);
    var cursor = t.find();
    for (i = 0; i < 1000; i++) {
        assert(cursor.hasNext());
        x = cursor.next();
        assert.eq(i, x._id);
        assert.eq(2*i, x.a);
    }
    assert(!cursor.hasNext());

    // test reverse table scan
    explain = t.find().sort({ _id : -1 }).explain();
    assert.eq(1000, explain.nscanned);
    cursor = t.find().sort({ _id : -1 });
    for (i = 0; i < 1000; i++) {
        assert(cursor.hasNext());
        x = cursor.next();
        assert.eq(999 - i, x._id);
        assert.eq(2*999 - 2*i, x.a);
    }
    assert(!cursor.hasNext());

    // test range scan
    explain = t.find({ _id: { $gte : 500, $lte : 750 }  }).hint({_id : 1}).explain();
    assert.eq(251, explain.nscanned)
    var cursor = t.find({ _id: { $gte : 500, $lte : 750 }  }).hint({_id : 1});
    for (i = 500; i <= 750; i++) {
        assert(cursor.hasNext());
        x = cursor.next();
        assert.eq(i, x._id);
        assert.eq(2*i, x.a);
    }
    assert(!cursor.hasNext());

    // test reverse table scan
    explain = t.find({ _id: { $gte : 500, $lte : 750 }  }).sort({ _id : -1 }).hint({_id : 1}).explain();
    assert.eq(251, explain.nscanned)
    cursor = t.find({ _id: { $gte : 500, $lte : 750 }  }).sort({ _id : -1 }).hint({_id : 1});
    for (i = 750; i >= 500; i--) {
        assert(cursor.hasNext());
        x = cursor.next();
        assert.eq(i, x._id);
        assert.eq(2*i, x.a);
    }
    assert(!cursor.hasNext());

    // test with a matcher
    explain = t.find({ _id: { $gte : 500, $lte : 750 }, a : {$gte : 1200}  }).hint({_id : 1}).explain();
    assert.eq(251, explain.nscanned)
    var cursor = t.find({ _id: { $gte : 500, $lte : 750 }, a : {$gte : 1200}  }).hint({_id : 1});
    for (i = 600; i <= 750; i++) {
        assert(cursor.hasNext());
        x = cursor.next();
        assert.eq(i, x._id);
        assert.eq(2*i, x.a);
    }
    assert(!cursor.hasNext());

    // reverse scan with a matcher
    explain = t.find({ _id: { $gte : 500, $lte : 750 }, a : {$gte : 1200}  }).sort({ _id : -1 }).hint({_id : 1}).explain();
    assert.eq(251, explain.nscanned)
    cursor = t.find({ _id: { $gte : 500, $lte : 750 }, a : {$gte : 1200}   }).sort({ _id : -1 }).hint({_id : 1});
    for (i = 750; i >= 600; i--) {
        assert(cursor.hasNext());
        x = cursor.next();
        assert.eq(i, x._id);
        assert.eq(2*i, x.a);
    }
    assert(!cursor.hasNext());

    // test projection
    cursor = t.find({}, {_id:1});
    for (i = 0; i < 1000; i++) {
        assert(cursor.hasNext());
        x = cursor.next();
        assert.eq(i, x._id);
    }
    assert(!cursor.hasNext());
    return true;
}


doWritesTest();
t.drop();
assert.commandWorked(db.runCommand({ create: 'part_coll_simple', partitioned:1}));
doQueriesTest();
t.drop();

assert.commandWorked(db.runCommand({ create: 'part_coll_simple', partitioned:1}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:100}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:200}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:300}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:400}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:500}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:600}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:700}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:800}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:900}}));
doWritesTest();
t.drop();
assert.commandWorked(db.runCommand({ create: 'part_coll_simple', partitioned:1}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:100}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:200}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:300}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:400}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:500}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:600}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:700}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:800}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:900}}));
doQueriesTest();
t.drop();

assert.commandWorked(db.runCommand({ create: 'part_coll_simple', partitioned:1}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:100}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:200}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:300}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:400}}));
assert.commandWorked(db.runCommand({addPartition:'part_coll_simple', newMax:{_id:500}}));
for (i = 201; i <=300; i++) {
    t.insert({_id:i});
}
for (i = 401; i <=500; i++) {
    t.insert({_id:i});
}
//test that cursors work over empty partitions
assert.eq(200, t.count());
assert.eq(100, t.count({_id: {$gt:50, $lt : 350}}));
assert.eq(100, t.count({_id: {$gt:350}}));
assert.eq(0, t.count({_id: {$gt:0, $lte : 200}}));
assert.eq(0, t.count({_id: {$gt:310, $lte : 390}}))
// test this going in reverse
cursor = t.find({_id: {$gt:0, $lte : 200}}).sort({_id:-1}).hint({_id : 1});
assert(!cursor.hasNext());
cursor = t.find({_id: {$gt:310, $lte : 390}}).sort({_id:-1}).hint({_id : 1});
assert(!cursor.hasNext());
cursor = t.find({_id: {$gt:350}}).sort({_id:-1}).hint({_id : 1});
for (i = 500; i > 400; i--) {
    assert(cursor.hasNext());
    assert.eq(cursor.next()._id, i);
}
assert(!cursor.hasNext());
cursor = t.find({_id: {$gt:50, $lt:350}}).sort({_id:-1}).hint({_id : 1});
for (i = 300; i > 200; i--) {
    assert(cursor.hasNext());
    assert.eq(cursor.next()._id, i);
}
assert(!cursor.hasNext());
cursor = t.find().sort({_id:-1}).hint({_id : 1});
for (i = 500; i > 400; i--) {
    assert(cursor.hasNext());
    assert.eq(cursor.next()._id, i);
}
for (i = 300; i > 200; i--) {
    assert(cursor.hasNext());
    assert.eq(cursor.next()._id, i);
}
assert(!cursor.hasNext());
t.drop();

