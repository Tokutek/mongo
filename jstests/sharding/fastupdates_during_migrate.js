// Test that fastupdates get written to the migrate log properly

// start up a new sharded cluster
var st = new ShardingTest({ shards : 2, mongos : 1 });

// stop balancer since we want manual control for this
st.stopBalancer();

var dbname = "testDB";
var coll = "fastupdate_during_migrate";
var ns = dbname + "." + coll;
var s = st.s0;
var t = s.getDB( dbname ).getCollection( coll );

// Create fresh collection with lots of docs
t.drop();
str = 'str';
while (str.length < 1024) str += str;
for (var i = 0; i < 100000; i++){
    if (i % 10000 == 0) {
        print("populating collection after sharding, " + i);
    }
    t.insert({ _id: i, c: 0, str: str });
}

// enable sharding of the collection. Only 1 chunk.
s.adminCommand( { enablesharding : dbname } );
s.adminCommand( { shardcollection : ns , key: { _id : 1 } } );

// start a parallel shell that does an $inc by _id (the pk here) every 10 documents
startMongoProgramNoConnect( "mongo" ,
                            "--host" , getHostName() ,
                            "--port" , st.s0.port ,
                            "--eval" , "sleep(2000); print('Doing update'); for (i = 0; i < 100000; i += 10) { if (i % 10000 == 0) { print(\"update \" + i); } db." + coll + ".update({ _id: i }, {'$inc': {'c': 1} }); assert.eq(null, db.getLastError()); } print('Update finished');" ,
                            dbname );

print('Moving chunks');
// migrate while fastupdates are happening
var moveResult =  s.adminCommand( { moveChunk : ns ,
                                    find : { a : 1 } ,
                                    to : st.getOther( st.getServer( dbname ) ).name } );
// check if migration worked
assert( moveResult.ok , "migration didn't work while doing updates" );
print('Chunk move finished');

// Every 10th doc should have 'c' incremented to 1, the rest should see it unchanged.
for (var i = 0; i < 100000; i++) {
    o = t.findOne({ _id: i  });
    assert.neq(null, o);
    if (i % 10000 == 0) print('checking i=' + i + ', ' + tojson(o));
    if (i % 10 == 0) {
        assert.eq({ _id: i, c: 1, str: str }, o);
    } else {
        assert.eq({ _id: i, c: 0, str: str }, o);
    }
}

st.stop();
