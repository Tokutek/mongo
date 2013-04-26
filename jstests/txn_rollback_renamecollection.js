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

function setupT() {
    db.T.insert({ a: 1 });
    db.T.insert({ b: 1 });
    db.T.insert({ c: 1 });
    db.T.insert({ z: 'magic' });
    db.T.ensureIndex({ z: 1 });
}

function setupU() {
    db.U.insert({ a: 1 });
    db.U.insert({ b: 1 });
    db.U.insert({ c: 1 });
    db.U.insert({ z: 'magic' });
    db.U.ensureIndex({ z: 1 });
}

function assertTDead() {
    assert.eq(0, db.system.namespaces.count({ 'name' : { $regex : 'T' } }));
    assert.eq(0, db.system.indexes.count({ 'ns' : { $regex : 'T' } }));
    assert.eq(0, db.T.count());
    assert.eq(0, db.T.count({ a: 1 }))
    assert.eq(0, db.T.count({ b: 1 }))
    assert.eq(0, db.T.count({ c: 1 }))
    assert.eq(0, db.T.count({ z: 'magic' }))
    assert.throws(db.T.find({ z : 'magic' }).hint({ z: 1 }).explain().cursor)
}
function assertUDead() {
    assert.eq(0, db.system.namespaces.count({ 'name' : { $regex : 'U' } }));
    assert.eq(0, db.system.indexes.count({ 'ns' : { $regex : 'U' } }));
    assert.eq(0, db.U.count());
    assert.eq(0, db.U.count({ a: 1 }))
    assert.eq(0, db.U.count({ b: 1 }))
    assert.eq(0, db.U.count({ c: 1 }))
    assert.eq(0, db.U.count({ z: 'magic' }))
    assert.throws(db.U.find({ z : 'magic' }).hint({ z: 1 }).explain().cursor)
}

function assertTOk() {
    assert.eq(3, db.system.namespaces.count({ 'name' : { $regex : 'T' } }));
    assert.eq(2, db.system.indexes.count({ 'ns' : { $regex : 'T' } }));
    assert.eq(4, db.T.count());
    assert.eq(1, db.T.count({ a: 1 }))
    assert.eq(1, db.T.count({ b: 1 }))
    assert.eq(1, db.T.count({ c: 1 }))
    assert.eq(1, db.T.count({ z: 'magic' }))
    assert.eq('IndexCursor z_1', db.T.find({ z : 'magic' }).hint({ z: 1 }).explain().cursor)
}
function assertUOk() {
    assert.eq(3, db.system.namespaces.count({ 'name' : { $regex : 'U' } }));
    assert.eq(2, db.system.indexes.count({ 'ns' : { $regex : 'U' } }));
    assert.eq(4, db.U.count());
    assert.eq(1, db.U.count({ a: 1 }))
    assert.eq(1, db.U.count({ b: 1 }))
    assert.eq(1, db.U.count({ c: 1 }))
    assert.eq(1, db.U.count({ z: 'magic' }))
    assert.eq('IndexCursor z_1', db.U.find({ z : 'magic' }).hint({ z: 1 }).explain().cursor)
}

setupT();
assertTOk();
assertUDead();

// try renaming T to U, then rollback
begin();
db.T.renameCollection('U');
assertTDead();
assertUOk();
rollback();
assertUDead();
assertTOk();

// now T and U exist.
setupU();

// try renaming T to V, U to T, then rollback
begin();
db.T.renameCollection('V');
assertTDead();
assertUOk();
db.U.renameCollection('T');
assertUDead();
assertTOk();
rollback();
assertUOk();
assertTOk();
assert.eq(0, db.V.count());

// test rename mixed with inserts, then rollback.
begin();
db.T.insert({ 'bad' : 1 })
db.T.renameCollection('V');
db.V.insert({ 'verybad' : 1 })
db.U.insert({ 'notsobad' : 1 })
rollback();
assertTOk();
assertUOk();
