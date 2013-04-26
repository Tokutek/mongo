function begin() {
    db.runCommand({ 'beginTransaction': 1 });
    assert(!db.getLastError());
}
function commit() {
    db.runCommand({ 'commitTransaction': 1 });
    assert(!db.getLastError());
}
function rollback() {
    db.runCommand({ 'rollbackTransaction': 1 });
    assert(!db.getLastError());
}

db.dropDatabase();
t = db.rollbackcreate;
t2 = db.rollbackcreate2;

// test database/collection create + commit
begin();
t.insert({ _id: 1 });
assert(!db.getLastError());
assert.eq(1, t.count())
assert.eq(2, db.system.namespaces.find({ "name" : { $regex: "rollbackcreate" } }).count());
commit();

t.drop();
assert(!db.getLastError());

// test rollback of database/collection create via insert
begin();
t.insert({ _id: 1 });
assert(!db.getLastError());
assert.eq(1, t.count())
assert.eq(2, db.system.namespaces.find({ "name" : { $regex: "rollbackcreate" } }).count());
rollback();
assert(!db.getLastError());
assert.eq(0, t.count())
assert.eq(0, db.system.namespaces.find({ "name" : { $regex: "rollbackcreate" } }).count());

t.drop();
assert(!db.getLastError());

// test rollback of multiple creates
begin();
t.insert({ _id: 1 });
assert(!db.getLastError());
assert.eq(1, t.count())
assert.eq(2, db.system.namespaces.find({ "name" : { $regex: "rollbackcreate" } }).count());
t2.insert({ _id: 1 })
assert(!db.getLastError());
assert.eq(1, t2.count())
assert.eq(4, db.system.namespaces.find({ "name" : { $regex: "rollbackcreate" } }).count());
assert(!db.getLastError());
rollback();
assert(!db.getLastError());
assert.eq(0, t.count())
assert.eq(0, t2.count())
assert.eq(0, db.system.namespaces.find({ "name" : { $regex: "rollbackcreate" } }).count());

// test that we can happily recreate t and t2 by normal means
t.insert({ _id: 1})
assert(!db.getLastError());
t2.insert({ _id: 1 })
assert(!db.getLastError());
assert.eq(1, t.count());
assert.eq(1, t2.count());

// test that aborting the create of t3 does not affect t nor t2
begin();
t3 = db.rollbackcreate3;
t3.insert({});
assert(!db.getLastError());
assert.eq(1, t.count());
assert.eq(1, t2.count());
assert.eq(1, t3.count());
assert.eq(6, db.system.namespaces.find({ "name" : { $regex: "rollbackcreate" } }).count());
rollback()
assert.eq(1, t.count());
assert.eq(1, t2.count());
assert.eq(0, t3.count());
assert.eq(4, db.system.namespaces.find({ "name" : { $regex: "rollbackcreate" } }).count());
