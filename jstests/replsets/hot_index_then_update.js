// Test simple fast updates

var replTest = new ReplSetTest({ name: 'hot_index_then_update', nodes: 3 });
var nodes = replTest.nodeList();

var conns = replTest.startSet();
var r = replTest.initiate({ "_id": "hot_index_then_update",
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

for (i = 0; i < 1000; i++) {
    primarydb.foo.insert({_id:i, a: 1, b :1});
}

primarydb.foo.ensureIndex({a:1},{background:true});
replTest.awaitReplication();
// now do an update where mods are indexed
primarydb.foo.update({_id:1},{$set :{a:2}});
replTest.awaitReplication();

// now do queries on secondary,
// make sure that the secondary index entry matches
// the one in the primary. This exposes #1087
x = secondarydb.foo.findOne({_id:1})
assert.eq(x.a, 2);
y = secondarydb.foo.find({a:2}).hint({a:1})
assert(y.hasNext());
z = y.next();
assert.eq(z._id, 1);

replTest.stopSet();
