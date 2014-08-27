// Tests partitioned and sharded collections

var st = new ShardingTest( { shards : 2, mongos : 1, separateConfig : 1  } );
// Stop balancer, it'll interfere
st.stopBalancer();

var admin = st.s.getDB( "admin" );
var db = st.s.getDB("foo");
var configs = st._configServers[0];

// create a partitioned collection, but not sharded
db.createCollection("foo", {partitioned : true});
assert.commandWorked(db.foo.getPartitionInfo());

// try sharding an existing partitioned collection and fail
assert.commandWorked(admin.runCommand({enableSharding : "foo"}));
assert.commandFailed(st.s.shardCollection("foo.foo", {a:1}));
db.foo.drop();

// fails because partitionKey must be a valid PK
assert.commandFailed(st.s.shardCollection("foo.foo", {a: 1}, false, true, {ts: 1}));
assert.commandWorked(st.s.shardCollection("foo.foo", {a: 1}, false, true, {ts: 1, _id: 1}));

// verify that we see foo.foo in the config and we can get a partition info
x = configs.getDB("config").collections.find({ _id : "foo.foo"}).next();
assert.eq(false, x.dropped);

assert.commandWorked(db.foo.getPartitionInfo());
x = db.foo.getPartitionInfo();
printjson(x);

// at this point, we know we have created a partitioned collection and that it is sharded
// it only exists on the first shard, at the moment
// now let's add some partitions
assert.commandWorked(db.foo.addPartition({ts : 10, _id: MaxKey}));
assert.commandWorked(db.foo.addPartition({ts : 20, _id: MaxKey}));
assert.commandWorked(db.foo.addPartition({ts : 30, _id: MaxKey}));
assert.commandWorked(db.foo.addPartition({ts : 40, _id: MaxKey}));
x = db.foo.getPartitionInfo();
printjson(x);

// now let's connect to the primary shard and verify that it has 5 partitions
var primary = st.getServer("foo");
var nonPrimary = st.getOther(primary);
x = primary.getDB("foo").foo.getPartitionInfo();
printjson(x);
assert.eq(x.numPartitions, 5);

// now let's do some insertions
for (var i = 1; i <= 50; i++) {
    db.foo.insert({_id : i, ts : i, a : 2*i, b : 3*i});
}
assert.eq( null, db.getLastError() );
assert.eq(db.foo.count(), 50);

// now verify that the indexes are right
x = primary.getDB("foo").foo.getIndexes();
assert.eq(x.length, 3); // _id, pk, shard key
assert.eq("primaryKey", x[0]["name"]);
assert.eq("_id_", x[1]["name"]);
assert.eq("a_1", x[2]["name"]);
// verify indexes are right
assert(friendlyEqual(x[0]["key"], {ts : 1, _id : 1}));
assert(friendlyEqual(x[1]["key"], {_id : 1}));
assert(friendlyEqual(x[2]["key"], {a : 1}));

// now test that a split and migrate work
assert.commandWorked(st.s.splitAt("foo.foo", {a : 62}));
assert.commandWorked(st.s.moveChunk("foo.foo", {a : 62}, nonPrimary.name));
assert.eq(30, primary.getDB("foo").foo.count());
assert.eq(20, nonPrimary.getDB("foo").foo.count());

x = nonPrimary.getDB("foo").foo.getPartitionInfo();
y = primary.getDB("foo").foo.getPartitionInfo();
// make sure that the partitions are the same on both shards, migrate should take care of this
assert(friendlyEqual(x,y));
// make sure indexes are the same on this second shard
x = nonPrimary.getDB("foo").foo.getIndexes();
y = primary.getDB("foo").foo.getIndexes();
assert(friendlyEqual(x,y));

// now let's drop some partitions
assert.commandWorked(db.foo.dropPartitionsLEQ({ts : 20, _id : MaxKey}));
// this should make it so that we now have 3 partitions
x = primary.getDB("foo").foo.getPartitionInfo();
printjson(x);
assert.eq(x.numPartitions, 3);
assert.eq(10, primary.getDB("foo").foo.count());
assert.eq(20, nonPrimary.getDB("foo").foo.count());
assert.eq(30, db.foo.count());
x = db.foo.find().sort({ts : 1}).next();
assert.eq(x.ts, 21);

x = nonPrimary.getDB("foo").foo.getPartitionInfo();
y = primary.getDB("foo").foo.getPartitionInfo();
// make sure that the partitions are the same on both shards, migrate should take care of this
assert(friendlyEqual(x,y));

// now let's do an add partition test
assert.commandWorked(db.foo.addPartition({ts : 50, _id : MaxKey}));
x = nonPrimary.getDB("foo").foo.getPartitionInfo();
assert.eq(x.numPartitions, 4);
y = primary.getDB("foo").foo.getPartitionInfo();
assert.eq(y.numPartitions, 4);
assert(friendlyEqual(x["partitions"][3]["max"], y["partitions"][3]["max"]));

st.stop();

