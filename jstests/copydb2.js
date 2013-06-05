a = db.getSisterDB( "copydb-test-a" );
b = db.getSisterDB( "copydb-test-b" );

a.dropDatabase();
b.dropDatabase();

numDocs = 15000;
for (i = 0; i < numDocs; i++) {
    a.foo.save( { a : 1 } );
}

assert.eq( numDocs , a.foo.count() , "A" );
assert.eq( 0 , b.foo.count() , "B" );

a.copyDatabase( a._name , b._name );

assert.eq( numDocs , a.foo.count() , "C" );
assert.eq( numDocs , b.foo.count() , "D" );
