// test writing to two db's at the same time.

t1 = db.jstests_basicc1;
t2 = db.jstests_basicc2;
t1.drop();
t2.drop();

var db = db.getSisterDB("test_basicc");
js = "for (i = 0; i < 20000; i++) { db.jstests_basicc1.save( {} ); }";
pid = startMongoProgramNoConnect( "mongo" , "--eval" , js , db.getMongo().host );
for( var i = 0; i < 20000; ++i ) {
    t2.save( {} );
}
assert.automsg( "!db.getLastError()" );
assert.soon( function() { return t1.count() == 20000 && t2.count() == 20000 }, 30, 10000 );

// put things back the way we found it
t1.drop();
t2.drop();
db.dropDatabase();
db = db.getSisterDB("test");
print('Done.');

