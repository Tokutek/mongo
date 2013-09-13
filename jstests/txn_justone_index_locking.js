// Test that justOne deletes / !multi updates do not
// overlock an index range on open-ended queries, github #485

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

t = db.txnjustonelocking;
t.drop();

t.insert({ _id: 0 });
t.insert({ _id: 1 });
t.insert({ _id: 2 });
t.insert({ _id: 3 });

// One thread updating a single document gte: 0 should not conflict with another
// updating a single document gte: 2 (because the justOne aspect should prevent
// prelocking 0 -> maxKey and 2 -> maxKey, respectively)

begin();
t.remove({ _id: { $gte: 0 } }, { justOne: true });
assert(!db.getLastError());
s1 = startParallelShell('db.txnjustonelocking.remove({ _id: { $gte: 2 } }, { justOne: true }); assert(!db.getLastError()); db.txnjustonelocking.insert({ success: "deleteJustOne2" }); assert(!db.getLastError());');
s1();
commit();
assert.eq(1, t.count({ 'success': "deleteJustOne2" }));
assert.eq(0, t.find({ _id: 0 }).itcount());
assert.eq(1, t.find({ _id: 1 }).itcount());
assert.eq(0, t.find({ _id: 2 }).itcount());
assert.eq(1, t.find({ _id: 3 }).itcount());

t.drop();
t.insert({ _id: 0 });
t.insert({ _id: 1 });
t.insert({ _id: 2 });
t.insert({ _id: 3 });
begin();
t.update({ _id: { $gte: 0 } }, { $set: { c: 1 } }, { multi: false });
assert(!db.getLastError());
s1 = startParallelShell('db.txnjustonelocking.update({ _id: { $gte: 2 } }, { $set: { c: 1 } }, { multi: false }); assert(!db.getLastError()); db.txnjustonelocking.insert({ success: "updateMultiFalse2" }); assert(!db.getLastError());');
s1();
commit();
assert.eq(1, t.count({ 'success': "updateMultiFalse2" }));
assert.eq(1, t.find({ _id: 0, c: 1 }).itcount());
assert.eq(1, t.find({ _id: 1 }).itcount());
assert.eq(1, t.find({ _id: 2, c: 1  }).itcount());
assert.eq(1, t.find({ _id: 3}).itcount());

t.drop();
t.insert({ _id: 0 });
t.insert({ _id: 1 });
t.insert({ _id: 2 });
t.insert({ _id: 3 });
begin();
t.findAndModify({ query: { _id: { $gte: 0 } }, update: { $set: { c: 1 } }});
assert(!db.getLastError());
s1 = startParallelShell('db.txnjustonelocking.findAndModify({ query: { _id: { $gte: 2 } }, update: { $set: { c: 1 } }}); assert(!db.getLastError()); db.txnjustonelocking.insert({ success: "findAndModify2" }); assert(!db.getLastError());');
s1();
commit();
assert.eq(1, t.count({ 'success': "findAndModify2" }));
assert.eq(1, t.find({ _id: 0, c: 1 }).itcount());
assert.eq(1, t.find({ _id: 1 }).itcount());
assert.eq(1, t.find({ _id: 2, c: 1  }).itcount());
assert.eq(1, t.find({ _id: 3}).itcount());
