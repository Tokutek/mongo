
t = db.stats1;
t.drop();

t.save( { a : 1 } );

assert.lt( 0 , t.dataSize() , "A" );
assert.lt( t.dataSize() , t.storageSize() , "B" );
assert.eq( 0 , t.totalIndexSize() , "C" );

assert.commandWorked(db.stats());
assert.commandWorked(db.stats(1024));
assert.commandWorked(db.stats({scale: 1024}));
assert.commandWorked(db.stats('1k'));
assert.commandWorked(db.stats({scale: '1k'}));
assert.commandWorked(db.coll.stats());
assert.commandWorked(db.coll.stats(1024));
assert.commandWorked(db.coll.stats({scale: 1024}));
assert.commandWorked(db.coll.stats('1k'));
assert.commandWorked(db.coll.stats({scale: '1k'}));
