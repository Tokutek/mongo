// Ensure that inserts and updates of the system.users collection validate the schema of inserted
// documents.
t = db.multi_key_mst_872;
t.drop();


function assertGLEOK(status) {
    assert(status.ok && status.err === null,
           "Expected OK status object; found " + tojson(status));
}

function assertGLENotOK(status) {
    assert(status.ok && status.err !== null,
           "Expected not-OK status object; found " + tojson(status));
}

t.ensureIndex( { a: 1 } );
t.insert( { a: [ 1, 2 ] } );
assertGLEOK(db.getLastErrorObj());
t.drop();

t.ensureIndex( { a: 1 } );
t.insert({_id:0});
t.update({_id:0}, { a: [ 1, 2 ] } );
assertGLEOK(db.getLastErrorObj());
t.drop();

t.ensureIndex( { a: 1 } );
db.beginTransaction();
t.insert( { a: [ 1, 2 ] } );
assertGLENotOK(db.getLastErrorObj());
db.rollbackTransaction();
t.drop();

t.ensureIndex( { a: 1 } );
t.insert({_id:0});
db.beginTransaction();
t.update({_id:0}, { a: [ 1, 2 ] } );
assertGLENotOK(db.getLastErrorObj());
db.rollbackTransaction();
t.drop();

