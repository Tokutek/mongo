t = db.jstests_count;

t.drop();
t.save( { i: 1 } );
t.save( { i: 2 } );
assert.eq( 1, t.find( { i: 1 } ).count(), "A"  );
assert.eq( 1, t.count( { i: 1 } ) , "B" );
assert.eq( 2, t.find().count() , "C" );
assert.eq( 2, t.find( undefined ).count() , "D" );
assert.eq( 2, t.find( null ).count() , "E" );
assert.eq( 2, t.count() , "F" );

t.drop();
t.save( {a:true,b:false} );
t.ensureIndex( {b:1,a:1} );
assert.eq( 1, t.find( {a:true,b:false} ).count() , "G" );
assert.eq( 1, t.find( {b:false,a:true} ).count() , "H" );

t.drop();
t.save( {a:true,b:false} );
t.ensureIndex( {b:1,a:1,c:1} );

assert.eq( 1, t.find( {a:true,b:false} ).count() , "I" );
assert.eq( 1, t.find( {b:false,a:true} ).count() , "J" );

t.drop();
t.save({ _id: 1 });
assert.eq(0, t.count({ _id: { $gt: 1 } })); // test $gt on non-inclusive lower bound
assert.eq(0, t.count({ _id: { $lt: 1 } })); // test $lt on non-inclusive upper bound
assert.eq(1, t.count({ _id: { $gte: 1 } })); // test $gt on inclusive lower bound
assert.eq(1, t.count({ _id: { $lte: 1 } })); // test $lt on inclusive upper bound

t.drop();
t.insert([{_id:1},{_id:2},{_id:3},{_id:4},{_id:5},{_id:6},{_id:7}]);
assert.eq(3, t.find({ _id : { $gt:2 , $lt:6} }).count()); // test double non-inclusive bounds
