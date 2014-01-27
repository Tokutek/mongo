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
var db2 = db.getSisterDB(db.getName() + 'test2');
var t = db.rollbackcapped;
var t2 = db2.rollbackcapped;
e = db.createCollection('rollbackcapped', { capped: true, size: 1024 })
assert(!db.getLastError());
db2.createCollection('rollbackcapped', { capped: true, size: 1024 })
assert(!db.getLastError());

function verifyEmpty(_t) {
    st = _t.stats()
    assert.eq( 0, st.cappedSizeCurrent ) // should not have changed
    assert.eq( 0, st.cappedCount ) // should not have changed
    assert.eq( 0, _t.count() );
}

verifyEmpty(t);
verifyEmpty(t2);

// test that a single insert can be rolled back
begin();
t.insert({});
rollback();
verifyEmpty(t);

// test that a single insert into two different dbs can be rolled back
begin();
t.insert({});
t2.insert({});
rollback();
verifyEmpty(t);
verifyEmpty(t2);
