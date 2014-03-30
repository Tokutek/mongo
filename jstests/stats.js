
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
assert.commandWorked(t.stats());
assert.commandWorked(t.stats(1024));
assert.commandWorked(t.stats({scale: 1024}));
assert.commandWorked(t.stats('1k'));
assert.commandWorked(t.stats({scale: '1k'}));
