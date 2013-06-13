// test that a big root level txn gets spilled
// run with --txnMemLimit 1000

db.test.drop();

// build an array of inserts of size n.
var n = 100;
var a = [];
for (var i=1; i<=n; i++) {
    a.push({i:i});
}
// insert them all
db.test.insert(a);

// verify 
assert.eq(n,db.test.count());
var s = 0;
db.test.find().forEach(function (x) { s+=x.i; });
assert.eq(s, n*(n+1)/2);

// TODO verify the oplog and oplog.refs



