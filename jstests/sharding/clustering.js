var st = new ShardingTest("clusteringtest", 2);
st.stopBalancer();
var s = st.s;
var admin = s.getDB("admin");
var db = s.getDB("clusteringtest");
var t = db.foo;

function doit(ensured, ensuredIsClustering,
              shardCommandSpecifiesClustering, shardCommandIsClustering, shardCommandShouldWork,
              keyShouldBeClustering) {
    for (var i = 0; i < 8; ++i) {
        db.dropDatabase();
        admin.runCommand({enableSharding: "clusteringtest"});
        if (i & 1) {
            t.insert({x:1});
        }
        if (ensured) {
            var obj = {x:1};
            if (i & 2) {
                obj.y = 1;
            }
            t.ensureIndex(obj, {clustering: ensuredIsClustering});
        }
        var cmd = {shardCollection: "clusteringtest.foo", key: {x:1}};
        if (shardCommandSpecifiesClustering) {
            cmd.clustering = shardCommandIsClustering;
        }
        if (shardCommandShouldWork && (ensured || !(i & 1))) {
            if (shardCommandSpecifiesClustering && (i & 4)) {
                sh.shardCollection("clusteringtest.foo", {x:1}, false, shardCommandIsClustering);
            }
            else {
                assert.commandWorked(admin.runCommand(cmd));
            }
            assert.eq(2, t.getIndexes().length);
            var idx = t.getIndexes()[1];
            assert.neq(null, idx);
            assert.eq(!!idx.clustering, keyShouldBeClustering);
        }
        else {
            assert.commandFailed(admin.runCommand(cmd));
            assert.eq(ensured ? 2 : 1, t.getIndexes().length);
        }
    }
}

// check that by default we make a clustering index
doit(false, false, false, false, true, true);
doit(false, false, true, true, true, true);

// check that we can make a non-clustering index
doit(false, false, true, false, true, false);

// check that we can shard on an existing index that is clustering
doit(true, true, false, false, true, true);
doit(true, true, true, true, true, true);

// check that we cannot shard on an existing index that is clustering if we don't want clustering
// (this is a weird case)
doit(true, true, true, false, false);

// check that we cannot shard on an existing index that is non-clustering
doit(true, false, false, false, false);
doit(true, false, true, true, false);

// check that we can shard on an existing index that is non-clustering, if we ask it to
doit(true, false, true, false, true, false);

st.stop();
