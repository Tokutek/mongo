// SERVER-1735 $type:10 matches null value, not missing value.

t = db.js966;
t.drop();

assert.commandWorked(db.runCommand({create:"js966", partitioned:1}));

x = db.system.namespaces.findOne( { name : "test.js966$$meta.$_id_" } );
assert( !x );
assert( !db.system.namespaces.findOne( { name : "test.js966$$p0.$_id_" } ) );
assert( db.system.namespaces.findOne( { name : "test.js966.$_id_" } ) );

t.drop();
