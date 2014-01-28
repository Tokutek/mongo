// test that unique checks on the pk may be manually disabled

t = db.pkunique;
t.drop();

// first insert is ok
t.insert({ _id: 0 });
assert.eq(null, db.getLastError());
// second insert is no tok
t.insert({ _id: 0 });
assert.neq(null, db.getLastError());
// insert is ok when unique chceks are disabled..
assert.commandWorked(db.adminCommand({setParameter: 1, pkUniqueChecks: false }));
t.insert({ _id: 0 });
assert.eq(null, db.getLastError());
// insert fails again with unique checks re-enabled
assert.commandWorked(db.adminCommand({setParameter: 1, pkUniqueChecks: true }));
t.insert({ _id: 0 });
assert.neq(null, db.getLastError());
