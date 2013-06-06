// Test removal of a substantial proportion of inserted documents.  SERVER-3803
// A complete test will only be performed against a DEBUG build.

t = db.jstests_removea;

Random.setRandomSeed();

t.drop();
t.ensureIndex( { a:1 } );
for( i = 0; i < 10000; ++i ) {
    t.save( { a:i } );
}

toDrop = [];
for( i = 0; i < 10000; ++i ) {
    toDrop.push( Random.randInt( 10000 ) ); // Dups in the query will be ignored.
}
// Remove many of the documents;
t.remove( { a:{ $in:toDrop }} );
assert( !db.getLastError() );
