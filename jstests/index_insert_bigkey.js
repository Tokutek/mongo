// Test that a big key fails an insert properly
t = db.insert;
t.drop();
t.ensureIndex({ a: 1 });
str = 's';
for (i = 0; i < 16; i++) {
    str += str;
}
t.insert({ a: str });
assert(db.getLastError());
assert.eq(0, t.count());
t.insert({ _id: str });
assert(db.getLastError());
assert.eq(0, t.count());
t.dropIndexes();
t.insert({ a: str });
assert(!db.getLastError());
assert.eq(1, t.count());
t.drop();
