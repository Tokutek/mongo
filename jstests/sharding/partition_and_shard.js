// Tests whether a split and a migrate in a sharded cluster preserve the epoch

var st = new ShardingTest( { shards : 2, mongos : 1, separateConfig : 1  } );
// Stop balancer, it'll interfere
st.stopBalancer();

var admin = st.s.getDB( "admin" );
var db = st.s.getDB("foo");
var configs = st._configServers[0];

// create a partitioned collection, but not going through mongos
db.createCollection("foo", {partitioned : true});
db.createCollection("bar");
assert.commandWorked(db.foo.getPartitionInfo());

// try sharding partitioned collection and fail
assert.commandWorked(admin.runCommand({enableSharding : "foo"}));

// just a sanity check that I can properly shard a collection
assert.commandWorked(sh.shardCollection("foo.bar", {a:1}));
// now verify that sharding an existing partitioned collection fails
assert.commandFailed(sh.shardCollection("foo.foo", {a:1}));
assert(db.foo.drop());

// now create the actual partitioned collection
assert.commandFailed(admin.runCommand({ shardCollection : "foo.foo", key : { a : 1 }, partition : {ts : 1} }));
assert.commandWorked(admin.runCommand({ shardCollection : "foo.foo", key : { a : 1 }, partition : {ts : 1, _id : 1} }));

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

// now let's connect to the first server and verify that it has 5 partitions
// not sure why the data ends up on _shardServers[1]. If this is random (and I don't think it is), test
// may fail and we need to be smarter about how we are getting this info
x = st._shardServers[1].getDB("foo").foo.getPartitionInfo();
printjson(x);
assert.eq(x.numPartitions, 5);

// now let's do some insertions
for (var i = 1; i <= 50; i++) {
    db.foo.insert({_id : i, ts : i, a : 2*i, b : 3*i});
}
assert.eq( null, db.getLastError() );
assert.eq(db.foo.count(), 50);

// now verify that the indexes are right
x = st._shardServers[1].getDB("foo").foo.getIndexes();
assert.eq(x.length, 3); // _id, pk, shard key
assert.eq("primaryKey", x[0]["name"]);
assert.eq("_id_", x[1]["name"]);
assert.eq("a_1", x[2]["name"]);
// verify indexes are right
assert(friendlyEqual(x[0]["key"], {ts : 1, _id : 1}));
assert(friendlyEqual(x[1]["key"], {_id : 1}));
assert(friendlyEqual(x[2]["key"], {a : 1}));

// now test that a split and migrate work
assert.commandWorked(sh.splitAt("foo.foo", {a : 62}));
assert.commandWorked(sh.moveChunk("foo.foo", {a : 62}, "shard0000"));
assert.eq(30, st._shardServers[1].getDB("foo").foo.count());
assert.eq(20, st._shardServers[0].getDB("foo").foo.count());

x = st._shardServers[0].getDB("foo").foo.getPartitionInfo();
y = st._shardServers[1].getDB("foo").foo.getPartitionInfo();
// make sure that the partitions are the same on both shards, migrate should take care of this
assert(friendlyEqual(x,y));
// make sure indexes are the same on this second shard
x = st._shardServers[0].getDB("foo").foo.getIndexes();
y = st._shardServers[0].getDB("foo").foo.getIndexes();
assert(friendlyEqual(x,y));

// now let's drop some partitions
assert.commandWorked(db.foo.dropPartitionsLEQ({ts : 20, _id : MaxKey}));
// this should make it so that we now have 3 partitions
x = st._shardServers[1].getDB("foo").foo.getPartitionInfo();
printjson(x);
assert.eq(x.numPartitions, 3);
assert.eq(10, st._shardServers[1].getDB("foo").foo.count());
assert.eq(20, st._shardServers[0].getDB("foo").foo.count());
assert.eq(30, db.foo.count());
x = db.foo.find().sort({ts : 1}).next();
assert.eq(x.ts, 21);

x = st._shardServers[0].getDB("foo").foo.getPartitionInfo();
y = st._shardServers[1].getDB("foo").foo.getPartitionInfo();
// make sure that the partitions are the same on both shards, migrate should take care of this
assert(friendlyEqual(x,y));

// now let's do an add partition test
assert.commandWorked(db.foo.addPartition({ts : 50, _id : MaxKey}));
x = st._shardServers[0].getDB("foo").foo.getPartitionInfo();
assert.eq(x.numPartitions, 4);
y = st._shardServers[1].getDB("foo").foo.getPartitionInfo();
assert.eq(y.numPartitions, 4);
assert(friendlyEqual(x["partitions"][3]["max"], y["partitions"][3]["max"]));

st.stop();

