// Test that inserting into a database after drop 
// works twice in a row. We had a bug where the first
// insert would create the nsindex in a read lock and then
// abort due to RetryWithWriteLock, but never cleared the
// in memory _nsdb pointer, causing another dropdb plus
// insert to use an outdated nsdb.
t = db.dropdatabasebug;

print('dropping database...');
db.dropDatabase();
e = db.getLastError();
if (e) print(e.toString());
assert(!e, "first dropDatabase");

print('inserting into t');
t.insert({});
e = db.getLastError();
if (e) print(e.toString());
assert(!e, "first insert");

print('dropping database again');
db.dropDatabase();
e = db.getLastError();
if (e) print(e.toString());
assert(!e, "second dropDatabase");

print('inserting into t again...');
t.insert({});
e = db.getLastError();
if (e) print(e.toString());
assert(!e, "second insert");

