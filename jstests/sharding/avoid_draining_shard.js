// Tests that a draining shard isn't chosen for new database primaries (#1069)

var options = {separateConfig : true, mongosOptions : {verbose : 1}};

var st = new ShardingTest({shards : 2, mongos : 1, other : options});

// Stop balancer initially
st.stopBalancer();

var mongos = st.s0;
var coll = mongos.getCollection("foo.bar");

assert.commandWorked(mongos.adminCommand({enableSharding : coll.getDB() + ""}));
assert.commandWorked(mongos.adminCommand({shardCollection : coll + "", key : {i : 1}}));

// Create a bunch of chunks
var numSplits = 20;
for ( var i = 0; i < numSplits; i++) {
    assert.commandWorked(mongos.adminCommand({split : coll + "", middle : {i : i}}));
}

var oldPrimary = st.getServer(coll.getDB() + "");

// Move one chunk to the other shard
assert.commandWorked(mongos.adminCommand({moveChunk: coll + "", find: {i: -1}, to: st.getOther(oldPrimary).name}))

// fill up that chunk so the other shard is bigger and would normally not be selected as the primary
for (var val = -1; i > -100000; --i) {
    coll.save({i: val, x: Math.random(), y: 'satonheus aotnehuasotnehusaonteuhaosnteuhaosnetuhaosnetuhaosnetuhaos', z: [Math.random(), Math.random(), Math.random()]});
}

// make sure we would select the first shard as primary
mongos.getCollection("newdb.bar").save({x: 1});
assert.eq(st.getServer("newdb").name, oldPrimary.name);
assert.commandWorked(mongos.getDB("newdb").dropDatabase());

// move the primary and start draining (balancer is off so no work will get done yet)
assert.commandWorked(mongos.adminCommand({movePrimary: coll.getDB() + "", to: st.getOther(oldPrimary).name}))
var res = mongos.adminCommand({removeShard: oldPrimary.name});
assert.commandWorked(res);
assert.eq(res.state, 'started');

// now that the shard is draining, check that a new db won't get it as primary
mongos.getCollection("newdb.bar").save({x: 1});
assert.neq(st.getServer("newdb").name, oldPrimary.name);
assert.commandWorked(mongos.getDB("newdb").dropDatabase());

// Start balancer, draining proceeds
st.startBalancer();

// Make sure we eventually drain, and keep checking that we don't get that shard as primary
assert.soon(function() {
    mongos.getCollection("newdb.bar").save({x: 1});
    assert.neq(st.getServer("newdb").name, oldPrimary.name);
    assert.commandWorked(mongos.getDB("newdb").dropDatabase());

    var res2 = mongos.adminCommand({removeShard: oldPrimary.name});
    assert.commandWorked(res);
    return res2.state == "completed";
}, "draining never finished", 5 * 60 * 1000);

// still shouldn't get it as primary, it should be done draining and out of the cluster
mongos.getCollection("newdb.bar").save({x: 1});
assert.neq(st.getServer("newdb").name, oldPrimary.name);
assert.commandWorked(mongos.getDB("newdb").dropDatabase());

st.stop();
