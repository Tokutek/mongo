// test that an insert and a drop in the same statement works with capped collections

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

t = db.rollbackcappeddrop;
t.drop();

db.createCollection('rollbackcappeddrop', { capped: true, size: 1024 })
assert(!db.getLastError());

t.insert({ a: 1 });
oldSize = t.stats().cappedSize;

begin();
t.insert({});
t.drop();
rollback();

assert.eq(1, t.count());
assert.eq(1, t.count({ a: 1 }));
assert.eq(oldSize, t.stats().cappedSize);
