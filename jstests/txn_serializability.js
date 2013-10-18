// Test that basic serializability can be achieved

var runTest = function(multiUpdate, multiDelete, iso) {
    t = db.txn_serializability;
    t2 = db.txn_serializability2;
    t.drop();
    t2.drop();
    t2.insert({}); // creates
    t.insert({ a: 1, b: 0 });
    t.insert({ a: 2, b: 0 });
    t.insert({ a: 3, b: 0 });
    t.ensureIndex({ a: 1, b: 1 }); // will cover a query on a, b so no locks in the _id index are taken on unmatched docs
    // I hate the multi-plan scanner and the inability to hint an update -_-
    assert.eq(2, t.find({ a: { $gte: 2 }, b: { $gte: 0 } }).itcount()); // should cache { a: 1, b: 1 } plan
    assert.commandWorked(db.beginTransaction(iso));
    t.update({ a: { $gte: 2 }, b: { $gte: 1 } }, { $set: { x: 1 } }, { multi: multiUpdate }); // won't match anything, won't take _id locks
    s1 = startParallelShell('db.txn_serializability.remove({ a: { $gte: 2 } }, ' +
                                '{ justOne: ' + (multiDelete ? 'false' : 'true') + '}); ' +
                            // Should get lock not granted since a: 2 is locked
                            'assert.neq(null, db.getLastError()); ' +
                            'db.txn_serializability2.insert({ _id: "failedAsPlanned" }); ' +
                            'assert.eq(null, db.getLastError());');
    s1();
    assert.commandWorked(db.commitTransaction());
    assert.eq(1, t2.count({ _id: "failedAsPlanned" }));
};

db.getSisterDB('admin').runCommand({ setParameter: 1, lockTimeout: 500 }); // no need to sleep for the default 4s
// The parameters (mvcc, false, false) are the most likely to expose a bug
[ 'mvcc', 'serializable' ].forEach( function(iso) {
    [ false, true ].forEach( function(multiUpdate) {
        [ false, true ].forEach( function(justOneDelete) {
            print('checking serializability ' + multiUpdate + ', ' + justOneDelete + ', ' + iso);
            runTest(multiUpdate, justOneDelete, iso);
        } )
    } )
} ) // John gains 100xp towards front-end development skillz
db.getSisterDB('admin').runCommand({ setParameter: 1, lockTimeout: 4000 });
