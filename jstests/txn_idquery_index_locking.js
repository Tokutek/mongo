// Test that updates/deletes with a point _id in the query only lock
// that one point in the index.

t = db.idquerylock;
t2 = db.idquerylock2;
t.drop();
t2.drop();

// The optimizer should _always_ use the _id index for point _id querires.
// We add an index on a: 1 to help expose bugs.
t.ensureIndex({ a: 1 });

var runTest = function(op, q1, q2, multi) {
    print('client op ' + op + ', q1 ' + tojson(q1) + ', q2 ' + tojson(q2) + ' multi ? ' + multi); 

    t.remove();
    t2.remove();
    t.insert({ _id: 0, a: 0 });
    t.insert({ _id: 1, a: 1 });
    t.insert({ _id: 2, a: 2 });
    t.insert({ _id: 3, a: 3 });
    assert.commandWorked(db.beginTransaction());
    t.update(q1, { $set: { z: 1 } }, { multi: multi });
    s1 = startParallelShell('db.idquerylock.' + (op == 'update' ? 'update' : 'remove') +
                                '(' + tojson(q2) + ', ' +
                                    (op == 'update' ? ('{ $set: { x: 1 } }, ' + '{ multi: ' + multi + ' });')
                                                    : ('{ justOne: ' + (multi ? false : true ) + ' });')) +
                            'assert.eq(null, db.getLastError());' +
                            'db.idquerylock2.insert({ _id: "success" });' +
                            'assert.eq(null, db.getLastError());');
    s1();
    assert.commandWorked(db.commitTransaction());
    assert.eq(1, t2.count({ _id: 'success' }));
};

// The idea is that no 'q1' should conflict with any 'q2'
// for any combination of who did remove, who passed multi: true, etc.
[ 'update', 'remove' ].forEach(function(op) {
    [ { _id: 2 }, { _id: 2, a: 2 }, { _id: 2, a: 3 } ].forEach(function(q1) {
        [ { _id: 3 }, { _id: 3, a: 3 } ].forEach(function(q2) {
            [ true, false ].forEach(function(multi) {
                runTest(op, q1, q2, multi);
            });
        });
    });
});
