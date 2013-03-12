
t = db.indexd;
t.drop();

t.save( { a : 1 } );
t.ensureIndex( { a : 1 } );
// TokuDB just returns false here instead of throwing an exception
//assert.throws( function(){ db.indexd.$_id_.drop(); } );
assert( !db.indexd.$_id_.drop() )
assert( t.drop() );

//db.indexd.$_id_.remove({});
