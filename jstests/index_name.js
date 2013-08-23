// Test that a long index names don't break the server

t = db.index_name;
t.drop();

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
