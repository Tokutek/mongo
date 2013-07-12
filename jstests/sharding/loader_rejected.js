// Test that begin/commit/abortLoad always get rejected by mongos

s = new ShardingTest( "shardload" , 2 );

db = s.getDB( "test" );
db.runCommand({ 'beginLoad' : 1 });
assert(db.getLastError());
db.runCommand({ 'commitLoad' : 1 });
assert(db.getLastError());
db.runCommand({ 'abortLoad' : 1 });
assert(db.getLastError());
