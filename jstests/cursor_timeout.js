// We want to test #826: that cursors get their idle age reset at the end of each getMore call and
// therefore can live longer than the cursor timeout as long as they keep receiving getMores.  To do
// this, we'll set the cursorTimeout low, and then run a long query with lots of getMores, and make
// sure we get all the way through.

t = db.cursor_timeout;
t.drop();
for (var i = 0; i < 50000; ++i) {
    t.insert({});
}
assert.eq(null, db.getLastError());

var res = db.adminCommand({getParameter: 1, cursorTimeout: 1});
assert.commandWorked(res);
oldCursorTimeout = res.cursorTimeout;
assert.commandWorked(db.adminCommand({setParameter: 1, cursorTimeout: 5000}));
try {
    startTime = new Date().getTime();
    assert.eq(50000, t.find(function(o) { sleep(1); return true; }).batchSize(100).itcount());
    assert.lt(30000, new Date().getTime() - startTime);
}
finally {
    res = db.adminCommand({setParameter: 1, cursorTimeout: oldCursorTimeout});
    assert.commandWorked(res);
}
