// Test that basic serializability can be achieved

var runTest = function(multiUpdate, multiDelete, iso) {
    t = db.txn_serializability;
    t2 = db.txn_serializability2;
    t.drop();
    t2.drop();
    t2.insert({}); // creates
    t.insert({ _id: 0, b: 0 });
    t.insert({ _id: 2, b: 0 });
    assert.commandWorked(db.beginTransaction(iso));
    // Work around the optimizer a bit...
    t.find({ _id: { $gte: 1 } }).hint({ _id: 1 }).itcount();
    assert.eq("IndexCursor _id_", t.find({ _id: { $gte: 1 } }).explain().cursor);
    // Should lock the keyspace from { 1 -> maxKey } if multi, { 1 -> 2 } otherwise.
    t.update({ _id: { $gte: 1 } }, { $set: { x: 1 } }, { multi: multiUpdate });
    s1 = startParallelShell('db.txn_serializability.remove({ _id: 1 }, ' +
                                '{ justOne: ' + (multiDelete ? 'false' : 'true') + '}); ' +
                            // Should get lock not granted since a: 1 is locked
                            'assert.neq(null, db.getLastError()); ' +
                            'db.txn_serializability.remove({ _id: 2 }, ' +
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
