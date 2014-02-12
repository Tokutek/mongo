// check that we don't scan $ne values

t = db.jstests_ne2;
t.drop();
t.ensureIndex( {a:1} );

t.save( { a:-0.5 } );
t.save( { a:0 } );
t.save( { a:0 } );
t.save( { a:0.5 } );

e = t.find( { a: { $ne: 0 } } ).explain( true );
<<<<<<< HEAD
assert.eq( "IndexCursor a_1 multi", e.cursor );
assert.eq( 0, e.indexBounds.a[ 0 ][ 1 ] );
assert.eq( 0, e.indexBounds.a[ 1 ][ 0 ] );
assert.eq( 3, e.nscanned );

e = t.find( { a: { $gt: -1, $lt: 1, $ne: 0 } } ).explain();
assert.eq( "IndexCursor a_1 multi", e.cursor );
assert.eq( { a: [ [ -1, 0 ], [ 0, 1 ] ] }, e.indexBounds );
assert.eq( 3, e.nscanned );
=======
assert.eq( 2, e.n, 'A' );

e = t.find( { a: { $gt: -1, $lt: 1, $ne: 0 } } ).explain();
assert.eq( 2, e.n, 'B' );
>>>>>>> b855e3e... SERVER-12532 negated predicates can use an index
