t = db.capped1;
t.drop();

db.createCollection("capped1" , {capped:true, size:1024 }); 
v = t.validate();
assert( v.valid , "A : " + tojson( v ) ); // SERVER-485

t.save( { x : 1 } )
assert( t.validate().valid , "B" )

// now do the same thing but test that BytesQuantity works too #545
t2 = db.capped1a;
t2.drop();
db.createCollection("capped1a", {capped:true, size:'1kb' });
print(db.getLastError());
v2 = t2.validate();
assert( v2.valid , "A2 : " + tojson( v2 ) );

t2.save( { x : 1 } )
assert( t2.validate().valid , "B2" )
