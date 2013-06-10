// Make sure serializable cursors that perfom reverse index scans prelock
// the appropriate key range. We'll know they did if we can 

t = db.serializable_reverse_cursor;
t.drop();

for (i = 0; i < 50; i++) {
    t.insert({});
}

db.runCommand({'beginTransaction':1, 'isolation':'serializable'})
assert(!db.getLastError());
count = t.find().sort( { $natural: -1 } ).itcount();
assert(!db.getLastError());
assert.eq(50, count);
db.runCommand({'commitTransaction':1})
assert(!db.getLastError());

db.runCommand({'beginTransaction':1, 'isolation':'serializable'})
assert(!db.getLastError());
count = t.find().sort( { $natural: 1 } ).itcount();
assert(!db.getLastError());
assert.eq(50, count);
db.runCommand({'commitTransaction':1})
assert(!db.getLastError());
