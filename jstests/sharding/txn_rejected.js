// Test that begin/commit/rollbackTransaction always get rejected by mongos

s = new ShardingTest( "shardtxn" , 2 );

db = s.getDB( "test" );
db.runCommand({ 'beginTransaction' : 1 });
assert(db.getLastError());
db.runCommand({ 'commitTransaction' : 1 });
assert(db.getLastError());
db.runCommand({ 'rollbackTransaction' : 1 });
assert(db.getLastError());
