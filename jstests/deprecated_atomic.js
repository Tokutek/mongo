// Test that remove/find still work with $atomic and $isolated,
// even though they are deprecated.

t = db.deprecatedatomic;
t.drop();

n = 10;

function runTest(q) {
    t.drop();
    for (i = 0; i < n; i++) {
        t.insert({});
    }
    t.remove(q);
    assert.eq(0, t.count(), 'remove query failed: ' + q);

    t.drop();
    for (i = 0; i < n; i++) {
        t.insert({});
    }
    x = t.find(q).itcount();
    assert.eq(n, x, 'find query failed: ' + q);
}

runTest({ $atomic : 1 });
runTest({ $isolated : 1 });
