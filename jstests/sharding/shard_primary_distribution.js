var st = new ShardingTest(testName = "shard_primary_distribution",
                          numShards = 4,
                          verboseLevel = 0,
                          numMongos = 1);

var fourk = "a";
while (fourk.length < 4096) {
    fourk += fourk;
}

for (var i = 0; i < 16; ++i) {
    var db = st.getDB('db' + i);
    for (var j = 0; j < 4096; ++j) {
        db.coll.insert({s: fourk});
    }
}

var databaseCounts = {};
for (var i = 0; i < 16; ++i) {
    var primaryShard = st.getServerName('db' + i);
    databaseCounts[primaryShard] = databaseCounts[primaryShard] || 0;
    databaseCounts[primaryShard]++;
}

var config = st.getDB('config');
var shards = config.shards.find().map(function(o) { return o._id; });
for (var i = 1; i < shards.length; ++i) {
    assert.eq(databaseCounts[shards[0]], databaseCounts[shards[i]], 'shards ' + shards[0] + ' and ' + shards[i] + ' are primaries for different #s of dbs');
}

st.stop();
