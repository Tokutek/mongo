// Test migrating a big chunk while updates are happening within that chunk.
// This tests that the migratelog works properly with large transactions.
// Test is slightly non-deterministic, since removes could happen before migrate
// starts. Protect against that by making chunk very large.

// start up a new sharded cluster
var st = new ShardingTest({ shards : 2, mongos : 1 });

// stop balancer since we want manual control for this
st.stopBalancer();

var dbname = "testDB";
var coll = "update_during_migrate";
var ns = dbname + "." + coll;
var s = st.s0;
var t = s.getDB( dbname ).getCollection( coll );

// Create fresh collection with lots of docs
t.drop();
for ( i=0; i<200000; i++ ){
    t.insert( { a : i } );
}

// enable sharding of the collection. Only 1 chunk.
t.ensureIndex( { a : 1 }, {clustering: true} );
s.adminCommand( { enablesharding : dbname } );
s.adminCommand( { shardcollection : ns , key: { a : 1 } } );

// start a parallel shell that deletes things
startMongoProgramNoConnect( "mongo" ,
                            "--host" , getHostName() ,
                            "--port" , st.s0.port ,
                            "--eval" , "sleep(2000); var bigstr = 'a'; while (bigstr.length < 1024) { bigstr += bigstr; }; print('Doing update'); db." + coll + ".update({}, {'$set': {'x': bigstr} }, {multi: true}); print('Update finished');" ,
                            dbname );

print('Moving chunk');
// migrate while deletions are happening
var moveResult =  s.adminCommand( { moveChunk : ns ,
                                    find : { a : 1 } ,
                                    to : st.getOther( st.getServer( dbname ) ).name } );
// check if migration worked
assert( moveResult.ok , "migration didn't work while doing updates" );
print('Chunk move finished');

t.find().forEach(function(o) { assert(o.x); assert(o.x.length == 1024); });

st.stop();
