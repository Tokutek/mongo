// Test that a big key fails an index build properly
t = db.buildbigkey;
t.drop();
str = 's';
for (i = 0; i < 16; i++) {
    str += str;
}
t.insert({ a: str });
t.ensureIndex({ a: 1 });
assert(db.getLastError());
