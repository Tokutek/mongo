// Test that fastUpdates get written to the migrate log properly

// start up a new sharded cluster
var st = new ShardingTest({ shards : 2, mongos : 1 });

// stop balancer since we want manual control for this
st.stopBalancer();

var dbname = "testDB";
var coll = "fastupdate_during_migrate";
var ns = dbname + "." + coll;
var s = st.s0;
var t = s.getDB( dbname ).getCollection( coll );
var chunks = s.getDB(dbname).chunks
print('chunks: ');
for (chunk in chunks.find().toArray()) {
    printjson(chunk);
}

// Create fresh collection with lots of docs
t.drop();
for (var i = 0; i < 100000; i++){
    if (i % 10000 == 0) {
        print("populating collection before sharding, " + i);
    }
    t.insert({ _id: i, a: i, b: i, c: 0 });
}

// enable sharding of the collection. Only 1 chunk.
s.adminCommand( { enablesharding : dbname } );
s.adminCommand( { shardcollection : ns , key: { _id : 1 }, numInitialChunks: 1 } );
var primary = st.getServer(dbname);
var nonPrimary = st.getOther(primary);
assert.commandWorked(primary.adminCommand({ setParameter: 1, fastUpdates: true }));
assert.commandWorked(nonPrimary.adminCommand({ setParameter: 1, fastUpdates: true }));

// start a parallel shell that does an $inc by _id (the pk here) every 10 documents
join = startParallelShell("db = db.getSiblingDB('" + dbname + "'); sleep(500); print('Doing update'); for (i = 0; i < 50000; i += 10) { if (i % 10000 == 0) { print(\"update \" + i); } db." + coll + ".update({ _id: i }, {'$inc': {'c': 1} }); assert.eq(null, db.getLastError()); } print('Update finished');", st.s0.port);

// migrate while fastUpdates are happening
var moveResult =  s.adminCommand( { moveChunk : ns ,
                                    find : { _id : 1 } ,
                                    to : st.getOther( st.getServer( dbname ) ).name } );
// check if migration worked
assert( moveResult.ok , "migration didn't work while doing updates" );
print('Chunk move finished');

// let updates finish
join();

// Every 10th doc should have 'c' incremented to 1, the rest should see it unchanged.
// We only update the first 50k docs so the test does not take forever.
var i = 0;
t.find().limit(50000).forEach(function(o) {
    assert.neq(null, o);
    if (i % 10000 == 0) print('checking i=' + i + ', ' + tojson(o));
    if (i % 10 == 0) {
        assert.eq({ _id: i, a: i, b: i, c: 1 }, o);
    } else {
        assert.eq({ _id: i, a: i, b: i, c: 0 }, o);
    }
    i++;
});

st.stop();
