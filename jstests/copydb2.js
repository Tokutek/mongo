a = db.getSisterDB( db.getName() + "-a" );
b = db.getSisterDB( db.getName() + "-b" );

a.dropDatabase();
b.dropDatabase();

for (i = 0; i < 1500; i++) {
    a.foo.save( { a : 1 } );
}

assert.eq( 1500 , a.foo.count() , "A" );
assert.eq( 0 , b.foo.count() , "B" );

a.copyDatabase( a._name , b._name );

assert.eq( 1500 , a.foo.count() , "C" );
assert.eq( 1500 , b.foo.count() , "D" );
