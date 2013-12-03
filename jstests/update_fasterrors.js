// test that fastupdate errors are noted in serverStatus
t = db.fastupdateserror;
t.drop();

t.insert({ _id: 0, str: 'string' });

var initialErrorCount = db.serverStatus().metrics.fastupdates.errors;

assert.commandWorked(db.adminCommand({ setParameter: 1, fastupdates: true }));
// Incrementing a string is an error
t.update({ _id: 0 } , { $inc: { str: 1 } });
assert.commandWorked(db.adminCommand({ setParameter: 1, fastupdates: false }));
// That error is silent because fastupdates were enabled.
assert.eq(null, db.getLastError());
// Reading _id: 0 will force the message application, triggering an error
assert.eq(1, t.find({ _id: 0 }).itcount());
// The error should have been recorded in fastupdates.errors
assert.lt(initialErrorCount, db.serverStatus().metrics.fastupdates.errors);
