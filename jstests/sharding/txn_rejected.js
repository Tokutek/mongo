// Test that begin/commit/rollbackTransaction always get rejected by mongos

s = new ShardingTest( "shardtxn" , 2 );

db = s.getDB( "test" );
assert.commandFailed(db.runCommand({ 'beginTransaction' : 1 }));
assert.commandFailed(db.runCommand({ 'commitTransaction' : 1 }));
assert.commandFailed(db.runCommand({ 'rollbackTransaction' : 1 }));
