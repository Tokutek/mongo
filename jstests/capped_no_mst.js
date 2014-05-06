// capped inserts cannot happen in a multi-statement transaction
t = db.cappednomst;
t.drop();
db.runCommand({ create: 'cappednomst', capped: true, size: 1024 });

// _id: 0 succeeds
t.insert({ _id: 0 });
assert.eq(null, db.getLastError());
assert.eq(1, t.count({ _id: 0 }));

// _id: 1 fails in a multi-statement transaction
db.beginTransaction();
t.insert({ _id: 1 });
assert.neq(null, db.getLastError());
db.commitTransaction();

// only _id: 0 exists
assert.eq(1, t.count({ _id: 0 }));
assert.eq(0, t.count({ _id: 1 }));
t.drop();
