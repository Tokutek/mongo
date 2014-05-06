// Test that we correctly use cursors when we hit the size limit for a showLiveTransactions or
// showPendingLockRequests command

var t = db.lock_diag_size_limit;
t.drop();

var s = "a";
while (s.length < 8*1024) {
    s += s;
}
t.ensureIndex({x:1});
// insert 80MB worth of key data, should blow up the buf builder
for (var i = 0; i < 10000; ++i) {
    t.insert({_id: s+i});
}
assert.eq(null, db.getLastError());

assert.commandWorked(db.beginTransaction());
t.update({}, {$set: {y:1}}, {multi: true});
assert.eq(null, db.getLastError());

// Test showLiveTransactions
var cursor = db.showLiveTransactions();
assert(cursor.hasNext());
var txns = cursor.toArray();
assert.eq(txns.length, 1);
// 16MB object can hold around 2000 8KB keys, but each lock is 2 keys, so this should be a little
// under 1000 locks returned, when you factor in overhead.  Should definitely be at least 500.
assert.gt(txns[0].rowLocks.length, 500);
// Definitely shouldn't be able to hold this many, we should have truncated the array.
assert.lt(txns[0].rowLocks.length, 5000);

db.rollbackTransaction();
t.drop();

// This way of testing showPendingLockRequests requires more open files than the buildslaves
// currently allow with ulimit.
if (0) {

// Test showPendingLockRequests
s = "a";
while (s.length < 32*1024) {
    s += s;
}
s = s.substring(0, 31 * 1024);
t.insert({});  // prevent mvcc dictionary too new
assert.commandWorked(db.beginTransaction());
t.insert({_id: s});
assert.eq(null, db.getLastError());

var thds = [];
for (var i = 0; i < 512; ++i) {
    thds.push(startParallelShell(
        'db = db.getSiblingDB("' + db.getName() + '");' +
        'var s = "a";' +
        'while (s.length < 32*1024) {' +
        '    s += s;' +
        '}' +
        's = s.substring(0, 31 * 1024);' +
        't = db.lock_diag_size_limit;' +
        't.insert({_id: s});' +
        'assert.neq(null, db.getLastError());'
    ));
}

var cursor = db.showPendingLockRequests();
assert(cursor.hasNext());
var reqs = cursor.toArray();
// We expect it to actually be around 256 when it hits the limit
assert.gt(reqs.length, 128);
assert.gt(reqs.length, 384);

for (var i = 0; i < 512; ++i) {
    thds[i]();
}

db.rollbackTransaction();
t.drop();

}
