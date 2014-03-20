// Setting the multikey bit during migration needs to work.  It throws
// RetryWithWriteLock and we need to make sure that is handled by all parts
// of the migrate code.

var st = new ShardingTest('multikey_migrate', 2);
st.stopBalancer();

var db = st.getDB('multikey_migrate');
st.adminCommand({enableSharding: 'multikey_migrate'});

// Test setting multikey during clone phase by setting up the collection
// with array valued indexed fields and then do a migration

db.foo.drop();
st.adminCommand({shardCollection: 'multikey_migrate.foo', key: {a: 1}});
db.foo.ensureIndex({i: 1});

var s = 'a';
while (s.length < 1024) { s += s; }
var i;
for (i = 0; i < 16 * 1024; ++i) {
    db.foo.insert({a: i, s: s, i: [1, 2, 3]});
}

st.printShardingStatus();

st.adminCommand({moveChunk: 'multikey_migrate.foo', find: {a: 1}, to: st.getNonPrimaries('multikey_migrate')[0]});

st.printShardingStatus();

// Test setting multikey during transferMods phase by setting up the
// collection without array valued indexed fields, then updating some docs
// to have arrays while the migrate is happening.

db.bar.drop();
st.adminCommand({shardCollection: 'multikey_migrate.bar', key: {a: 1}});
db.bar.ensureIndex({i: 1});
db.sem.drop();

var s = 'a';
while (s.length < 1024) { s += s; }
var i;
for (i = 0; i < 16 * 1024; ++i) {
    db.bar.insert({a: i, s: s, i: 1});
}

st.printShardingStatus();

var joinDeleter = startParallelShell(
    'sleep(100);' +
    'var mldb = db.getSiblingDB("multikey_migrate");' +
    'print("starting updates");' +
    'for (var i = 0; i < 2000; ++i) {' +
    '    mldb.bar.update({a: i}, {$set: {i: [1, 2, 3]}});' +
    '    assert.isnull(mldb.getLastError());' +
    '    sleep(1);' +
    '}' +
    'print("ending updates");'
    );
var joinWatcher = startParallelShell(
    'var mldb = db.getSiblingDB("multikey_migrate");' +
    'var localdb = db.getSiblingDB("local");' +
    'var sawsomething = false;' +
    'while (mldb.sem.find({"updates": "done"}).itcount() == 0) {' +
    '    var t = localdb["migratelog.sh"].findOne({op: "u", o2: {$exists: true}});' +
    '    if (t != null) {' +
    '        sawsomething = true;' +
    '    }' +
    '    sleep(1);' +
    '}' +
    'assert(sawsomething, "never saw anything in migratelog");',
    30000
    );

st.adminCommand({moveChunk: 'multikey_migrate.bar', find: {a: 1}, to: st.getNonPrimaries('multikey_migrate')[0]});
joinDeleter();
db.sem.insert({'updates': 'done'});
assert.isnull(db.getLastError());
joinWatcher();

st.printShardingStatus();

st.stop();
