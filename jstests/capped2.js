db.capped2.drop()
db._dbCommand( { create: "capped2", capped: true, size: 1000, $nExtents: 11, autoIndexId: false } );
tzz = db.capped2;

function debug( x ) {
    print( x );
}

var val = new Array( 2000 );
var c = "";
for( i = 0; i < 2000; ++i, c += "---" ) { // bigger and bigger objects through the array...
    val[ i ] = { a: c };
}

function checkIncreasing( i ) {
    res = tzz.find().sort( { $natural: -1 } );
    assert( res.hasNext(), "A" );
    var j = i;
    while( res.hasNext() ) {
        try {
            assert.eq( val[ j-- ].a, res.next().a, "B" );
        } catch( e ) {
            debug( "capped2 err " + j );
            throw e;
        }
    }
    res = tzz.find().sort( { $natural: 1 } );
    assert( res.hasNext(), "C" );
    while( res.hasNext() )
	assert.eq( val[ ++j ].a, res.next().a, "D" );
    assert.eq( j, i, "E" );
}

function checkDecreasing( i ) {
    res = tzz.find().sort( { $natural: -1 } );
    assert( res.hasNext(), "F" );
    var j = i;
    while( res.hasNext() ) {
      	assert.eq( val[ j++ ].a, res.next().a, "G" );
    }
    res = tzz.find().sort( { $natural: 1 } );
    assert( res.hasNext(), "H" );
    while( res.hasNext() )
	assert.eq( val[ --j ].a, res.next().a, "I" );
    assert.eq( j, i, "J" );
}

for( i = 0 ;; ++i ) {
    debug( "capped 2: " + i );
    tzz.insert( val[ i ] );
    if ( i > 1 && tzz.count() == 1 ) {
    	assert( i > 100, "K" );
        break;
    }
    checkIncreasing( i );
}

// val[159] won't fit in the capped collection,
// so check decreasing from 158 and under.
for( i = 158 ; i >= 0 ; --i ) {
    debug( "capped 2: " + i );
    tzz.insert( val[ i ] );
    checkDecreasing( i );
}

// the last object generated is too big for the capped collection itself.
countBefore = tzz.count();
assert.throws( tzz.insert( val[1999] ) );
assert.eq( tzz.count(), countBefore );
