st = new ShardingTest("migratelog", 2);
st.stopBalancer();

var db = st.getDB("migratelogtest");
st.adminCommand({enableSharding: "migratelogtest"});
db.foo.drop();
db.foo.ensureIndex({_id: 1}, {clustering: true});
st.adminCommand({shardCollection: "migratelogtest.foo", key: {'_id': 1}});

var s = "a";
while (s.length < 1024) { s += s; }
var i;
for (i = 0; i < 16 * 1024; ++i) {
    db.foo.insert({'_id': i, 's': s});
}

//st.adminCommand({split: "migratelogtest.foo", middle: {'_id': Math.floor(i/2)}});
st.printShardingStatus();

var joinDeleter = startParallelShell(
    'var mldb = db.getSiblingDB("migratelogtest");' +
    'print("starting deletes");' +
    'for (var i = 0; i < 2000; ++i) {' +
    '    mldb.foo.remove({"_id": i});' +
    '    assert.isnull(mldb.getLastError());' +
    '    sleep(1);' +
    '}' +
    'print("ending deletes");'
    );
var joinWatcher = startParallelShell(
    'var mldb = db.getSiblingDB("migratelogtest");' +
    'var localdb = db.getSiblingDB("local");' +
    'var sawsomething = false;' +
    'while (mldb.sem.find({"deletes": "done"}).itcount() == 0) {' +
    '    var t = localdb["migratelog.sh"].findOne();' +
    '    if (t != null) {' +
    '        sawsomething = true;' +
    '    }' +
    '    sleep(1);' +
    '}' +
    'assert(sawsomething, "never saw anything in migratelog");',
    30000
    );

st.adminCommand({moveChunk: "migratelogtest.foo", find: {'_id': 0}, to: st.getNonPrimaries("migratelogtest")[0]});
joinDeleter();
db.sem.insert({'deletes': 'done'});
assert.isnull(db.getLastError());
joinWatcher();

st.printShardingStatus();

for (var i = 0; i < 2000; ++i) {
    assert.isnull(db.foo.findOne({'_id': i}), "element " + i + " didn't get deleted");
}
assert.neq(null, db.foo.findOne({'_id': 2000}), "element 2000 got lost");
