// test that a big nested txn gets spilled
// run with --txnMemLimit 1000

db.test.drop();

db.runCommand("beginTransaction")
for (var i=1; i<=100; i++) {
    db.test.insert({i:i});
}
db.runCommand("commitTransaction")

assert.eq(100,db.test.count());
var s = 0;
db.test.find().forEach(function (x) { s+=x.i; });
assert.eq(s, n*(n+1)/2);

// TODO verify the oplog and oplog.refs

