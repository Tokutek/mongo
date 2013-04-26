// test that dropping a database with closed namespaces
// works as expected. the rename will close the resulting
// namespace, which exercises the case.
t = db.dropdatabasebug2;

print('dropping t');
t.drop();
e = db.getLastError();
if (e) print(e.toString());
assert(!e, "first drop");

print('inserting into t');
t.insert({});
e = db.getLastError();
if (e) print(e.toString());
assert(!e, "first insert into t");

print('renaming t to u');
t.renameCollection('u');
e = db.getLastError();
if (e) print(e.toString());
assert(!e, "rename to u");

print('dropping database...');
db.dropDatabase();
e = db.getLastError();
if (e) print(e.toString());
assert(!e, "first dropDatabase");

print('inserting into t again');
t.insert({});
e = db.getLastError();
if (e) print(e.toString());
assert(!e, "second insert into t");

