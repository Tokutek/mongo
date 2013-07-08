// in 1.0.1, deadlocks and lock wait timeouts killed the connection, rather than just killing the current operation
// we need to test that they don't kill the connection any more

var t = db.bug301;
t.drop();
t.insert({}); // collection has to exist, otherwise the transaction we create will have a write lock on the collection and we can't insert {passed:true}

db.runCommand('beginTransaction');
assert.eq(null, db.getLastError());

t.insert({_id:1});
assert.eq(null, db.getLastError());

var join = startParallelShell('db.bug301.insert({_id:1}); assert.neq(null, db.getLastError()); db.bug301.insert({passed: true}); assert.eq(null, db.getLastError());');
var x = join();
db.runCommand('commitTransaction');
assert.eq(null, db.getLastError());
assert.eq(1, t.find({passed:true}).itcount());

t.drop();
