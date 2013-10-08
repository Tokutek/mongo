// Test that showing live transactions and lock requests
// gets rejected by mongos

s = new ShardingTest( "shardshowtransactions" , 2 );

db = s.getDB( "test" );
assert.commandFailed(db.runCommand({ 'showLiveTransactions' : 1 }));
assert.commandFailed(db.runCommand({ 'showPendingLockRequests' : 1 }));
