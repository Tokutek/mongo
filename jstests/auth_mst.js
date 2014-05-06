users = db.getCollection( "system.users" );

// If system.users is not already open, then the query inside addUser will be done with the MST and
// will take a row lock on the collection name, and then the write (in an alternate transaction
// stack) will fail to acquire the row lock in the CollectionMap.
// We don't really care about this case, but we want to be able to run each test in its own DB if we
// want, so we just make sure the collection is open here, before we begin the MST.
db.createCollection("system.users");
users.remove( {} );
db.auth_mst.drop();
assert.eq(db.auth_mst.count(), 0);

db.auth_mst.insert( { a:1 } );
db.runCommand("beginTransaction");
db.auth_mst.insert( { a:1 } );
db.addUser( "eliot" , "eliot" );
db.runCommand("rollbackTransaction");

assert.eq(db.auth_mst.count(), 1);
assert.eq(db.system.users.count(), 1);
assert( db.auth( "eliot" , "eliot" ) , "auth failed" );

db.runCommand("beginTransaction");
db.auth_mst.insert( { a:1 } );
db.removeUser( "eliot" );
db.runCommand("rollbackTransaction");

assert.eq(db.auth_mst.count(), 1);
assert.eq(db.system.users.count(), 0);
