// Test that begin/commit/abortLoad always get rejected by mongos

s = new ShardingTest( "shardload" , 2 );

db = s.getDB( "test" );
assert.commandFailed(db.runCommand({ 'beginLoad' : 1 }));
assert.commandFailed(db.runCommand({ 'commitLoad' : 1 }));
assert.commandFailed(db.runCommand({ 'abortLoad' : 1 }));
