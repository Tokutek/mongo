// Test that various operations work properly after a database is known to be closed.
// Bugs include not using a ReadContext to handle opening an unopened DB while in a read lock.
// It would also be a bug to have a nested read lock when trying to open the database.
t = db.ops_after_close
t.drop();

function dropdb() {
    db.dropDatabase(); // aborting a dropDatabases closes the db
    assert(!db.getLastError());
}

dropdb();
t.insert({ c: 1 });
assert(!db.getLastError());

dropdb();
t.update({}, { $set : { z : 1 } });
assert(!db.getLastError());

dropdb();
t.remove({ z: 1 }, false);
assert(!db.getLastError());

dropdb();
assert.eq(0, t.count());
assert(!db.getLastError());

dropdb();
c = t.find({});
assert(!db.getLastError());

dropdb();
c = t.find({ k: 1 });
assert(!db.getLastError());

dropdb();
t.ensureIndex({ h: 1 });
assert(!db.getLastError());

dropdb();
t.dropIndexes();
assert(!db.getLastError());

dropdb();
t.stats();
assert(!db.getLastError());

dropdb();
db.runCommand({'findAndModify':'ops_after_close',
               'query': {},
               'update': { $set : { findandmodify : 10 } }});
assert(!db.getLastError());

dropdb();
db.runCommand({'findAndModify':'ops_after_close',
               'query': { z: 1 },
               'sort' : { k: 1 }, // sort goes through a different code path
               'update': { $set : { findandmodify : 10 } },
               'upsert': true });
assert(!db.getLastError());
