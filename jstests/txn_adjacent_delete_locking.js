// Test that we can delete adjacent _id index keys without lock conflict
// See TokuMX github issue #651

var runTest = function(justOne, iso) {
    t = db.txn_adjacent_id_delete;
    t.drop();

    t.insert({ _id: 0 });
    t.insert({ _id: 1 });
    t.insert({ _id: 2 });
    t.insert({ _id: 3 });
    assert.eq(4, t.count());

    assert.commandWorked(db.beginTransaction(iso));
    t.remove({ _id: 1 }, { 'justOne' : justOne });
    s1 = startParallelShell('db.txn_adjacent_id_delete.remove({ _id: 2 }, ' +
                                '{ justOne: ' + (justOne ? 'true' : 'false') + '}); ' +
                            'assert.eq(null, db.getLastError()); ' +
                            'db.txn_adjacent_id_delete.insert({ success: 1 }); ' +
                            'assert.eq(null, db.getLastError());');
    s1();
    assert.commandWorked(db.commitTransaction());
    assert.eq(1, t.count({ success: 1 }));
    assert.eq(0, t.count({ _id: 1 }));
    assert.eq(0, t.count({ _id: 2 }));
}

runTest(true, 'serializable');
runTest(true, 'mvcc');
runTest(false, 'serializable');
runTest(false, 'mvcc');
