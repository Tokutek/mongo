// Test that neither bogus nor long index names break the server.

t = db.index_name;
t.drop();

assert.throws(t.ensureIndex({ a: 1 }, { name: "" }));
assert.throws(t.ensureIndex({ a: 1 }, { name: 1 }));
assert.throws(t.ensureIndex({ a: 1 }, { name: 1.7 }));
assert.throws(t.ensureIndex({ a: 1 }, { name: { bad: 1 } }));
assert.throws(t.ensureIndex({ a: 1 }, { name: [ { bad: "name" } ] }));

n = "n";
for (var k = 0; k < 16; k++) {
    t.ensureIndex({ a: 1 }, { name: n });
    if (k < 8) {
        // Lengths up to 128 should work.
        // > 128 may or may not work, depending on filesystem.
        assert(!db.getLastError());
    }
    t.dropIndexes();
    assert(!db.getLastError());
    n += n;
}
