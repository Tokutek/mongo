// Test simple fast updates

var replTest = new ReplSetTest({ name: 'fastupdates', nodes: 3 });
var nodes = replTest.nodeList();

var conns = replTest.startSet();
var r = replTest.initiate({ "_id": "fastupdates",
                            "members": [
                                { "_id": 0, "host": nodes[0], priority:10 },
                                { "_id": 1, "host": nodes[1] },
                                { "_id": 2, "host": nodes[2], arbiterOnly: true}
                            ]});

// Make sure we have a primary
var primary = replTest.getMaster();
b_conn = conns[1];
b_conn.setSlaveOk();

var primarydb = primary.getDB('db');
var secondarydb = b_conn.getDB('db');

primarydb.fastupdates.drop();
primarydb.fastupdates.ensureIndex({ a: 1 });
primarydb.fastupdates.ensureIndex({ b: 1 });
for (i = 0; i < 1000; i++) {
    // Leave a gap every third doc. The upserts below will need to fill them in.
    if (i % 3 != 0) {
        primarydb.fastupdates.insert({ _id: i, a: i, b: i, c: 0, str: 'stringystringstring!' });
    }
}
for (i = 0; i < 1000; i++) {
    // Use a fast update for every other update.
    var fast = i % 2 == 0;
    if (fast) {
        assert.commandWorked(primarydb.adminCommand({ setParameter: 1, fastupdates: true }));
    }
    primarydb.fastupdates.update({ _id: i }, { $inc: { c: 1 } }, { upsert: true });
    if (fast) {
        assert.commandWorked(primarydb.adminCommand({ setParameter: 1, fastupdates: false }));
    }
}
assert.eq(1000, primarydb.fastupdates.count({ c: 1 }));
replTest.awaitReplication();
assert.eq(1000, primarydb.fastupdates.count({ c: 1 }));
replTest.stopSet();
