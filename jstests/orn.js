// Test dropping during an $or distinct yield SERVER-3555
assert(false, "orm test disabled: it says it tests a yield but runs for a very long time (15m), not sure if it's valid for toku");

t = db.jstests_orn;
t.drop();

clauses = [];
for( i = 0; i < 10; ++i ) {
    clauses.push( {a:{$lte:(i+1)*5000/10},i:49999} );
    clauses.push( {b:{$lte:(i+1)*5000/10},i:49999} );
}

p = startParallelShell( 'for( i = 0; i < 15; ++i ) { sleep( 1000 ); db.jstests_orn.drop() }' );
for( j = 0; j < 5; ++j ) {
    for( i = 0; i < 5000; ++i ) {
        t.save( {a:i,i:i} );
        t.save( {b:i,i:i} );
    }
    t.ensureIndex( {a:1} );
    t.ensureIndex( {b:1} );
    t.distinct('a',{$or:clauses});
}
p();
