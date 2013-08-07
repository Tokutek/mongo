users = db.getCollection( "system.users" );
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
