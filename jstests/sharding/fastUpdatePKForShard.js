// Tests partitioned and sharded collections
assertUpdateFast = function(wdb, localdb) {
    y = localdb.oplog.rs.find().sort({$natural : -1}).next();
    printjson(y);
    assert(y["ops"][0]["f"] > 0); // means that update was not fast. 1 or 3 is fast
    assert.eq(y["ops"][0]["op"], "ur");
    assert.eq(undefined, y["ops"][0]["o"]);
}

assertUpdateSlow = function(wdb) {
    x = wdb.runCommand({getLastError : 1});
    assert.eq(x.ok, 1);
    assert.eq(x.updatedExisting, false);
    assert.eq(x.n, 0);
};

assertOplogEntrySlow = function(localdb) {
    y = localdb.oplog.rs.find().sort({$natural : -1}).next();
    assert.eq(y["ops"][0]["f"], 0); // means that update was not fast. 1 or 3 is fast
    assert.eq(y["ops"][0]["op"], "ur");
    assert.eq(undefined, y["ops"][0]["o"]);
}


var st = new ShardingTest( { shards : { rs0: { nodes : 1 }}, mongos : 1, separateConfig : 1  } );
// Stop balancer, it'll interfere
st.stopBalancer();

var admin = st.s.getDB( "admin" );
var db = st.s.getDB("foo");
var configs = st._configServers[0];
assert.commandWorked(admin.runCommand({enableSharding : "foo"}));
var primary = st.getServer("foo");
var localdb = primary.getDB("local");


// create a not sharded collection
assert.commandWorked(db.createCollection("foo"));
// first basic test, that fast updates work only when enabled
db.foo.insert({_id : 0, a : 0});
//update of non-existent key should not work
db.foo.update({_id : 1}, { $set : {a : 1}});
assertUpdateSlow(db);
// now let's do an update that works, verify that it is slow
db.foo.update({_id : 0}, { $set : {a : 1}});
assertOplogEntrySlow(localdb);

// now let's turn on fast updates, do all the above things, and verify that they are indeed fast
// this is a sanity check to verify that the setParameter works
assert.commandWorked(primary.adminCommand({ setParameter: 1, fastupdates: true }));
db.foo.update({_id : 0}, { $set : {a : 100}});
assertUpdateFast(db, localdb);
db.foo.drop();

// now let's show that if the pk is not included in the shard key, updates are slow
assert.commandWorked(db.createCollection("foo"));
db.foo.ensureIndex({b : 1});
db.foo.insert({_id : 0, a : 0, b : 0});
assert.commandWorked(db.adminCommand({ shardCollection: 'foo.foo', key: { b : 1 }, clustering: false}));
db.foo.update({_id : 0}, { $inc : {a : 102}});
assertOplogEntrySlow(localdb); // verify the slow update, because the shard key, b, is not included in the pk
db.foo.drop();

// now lets show that when the pk has the shard key, we can do a fast update
assert.commandWorked(sh.shardCollection("foo.foo", {a : 1}));
db.foo.insert({_id : 0, a : 0, b : 1});
db.foo.update({_id : 0, a : 0}, { $inc : {b : 100}});
db.getLastError();
assertUpdateFast(db, localdb);
db.foo.drop();

assert.commandWorked(db.adminCommand({ shardCollection: 'foo.foo', key: { _id : "hashed" }, clustering: false}));
db.foo.insert({_id : 0, a : 0});
db.foo.update({_id : 0}, { $inc : {a : 100}});
x = db.foo.find({_id : 0}).next();
assert.eq(x.a, 100);
assertUpdateFast(db, localdb);
db.foo.drop();

st.stop();

