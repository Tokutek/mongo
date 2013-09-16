// Test that showing live transactions and lock requests
// gets rejected by mongos

s = new ShardingTest( "shardshowtransactions" , 2 );

db = s.getDB( "test" );
db.runCommand({ 'showLiveTransactions' : 1 });
assert(db.getLastError());
db.runCommand({ 'showPendingLockRequests' : 1 });
assert(db.getLastError());
