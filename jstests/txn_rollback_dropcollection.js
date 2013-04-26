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
t = db.rollbackdropcollection;
t.insert({ a: 1 });
t.insert({ b: 1 });
t.insert({ c: 1 });
t.insert({ z: 'magic' });
t.ensureIndex({ z: 1 });

function assertDead() {
    assert.eq(0, db.system.namespaces.count({ 'name' : { $regex : 'rollbackdropcollection' } }));
    assert.eq(0, db.system.indexes.count({ 'ns' : { $regex : 'rollbackdropcollection' } }));
    assert.eq(0, t.count());
    assert.eq(0, t.count({ a: 1 }))
    assert.eq(0, t.count({ b: 1 }))
    assert.eq(0, t.count({ c: 1 }))
    assert.eq(0, t.count({ z: 'magic' }))
    assert.throws(t.find({ z : 'magic' }).hint({ z: 1 }).explain().cursor)
}
function assertOk() {
    assert.eq(3, db.system.namespaces.count({ 'name' : { $regex : 'rollbackdropcollection' } }));
    assert.eq(2, db.system.indexes.count({ 'ns' : { $regex : 'rollbackdropcollection' } }));
    assert.eq(4, t.count());
    assert.eq(1, t.count({ a: 1 }))
    assert.eq(1, t.count({ b: 1 }))
    assert.eq(1, t.count({ c: 1 }))
    assert.eq(1, t.count({ z: 'magic' }))
    assert.eq('IndexCursor z_1', t.find({ z : 'magic' }).hint({ z: 1 }).explain().cursor)
}
assertOk();

// try rolling back a drop
begin();
t.drop();
assertDead();
rollback();
assertOk();

// try rolling back a drop followed by an insert
begin();
t.drop();
assertDead();
t.insert({ 'followed by an insert' : 700 });
assert.eq(1, t.count());
rollback();
assertOk();

// try rolling back a drop followed and proceeded by an insert
begin();
t.insert({ 'proceeded by an insert' : 699 })
t.drop();
assertDead();
t.insert({ 'followed by an insert' : 701 })
assert.eq(1, t.count());
rollback();
assertOk();

// try rolling back two drops
t2 = db.somethingelse;
t2.drop();
t2.insert({ 'q' : 'p' });
assert.eq(1, t2.count());
begin();
t.drop();
assertDead();
t2.drop();
assert.eq(0, t2.count());
rollback();
assertOk();
assert.eq(1, t2.count());
