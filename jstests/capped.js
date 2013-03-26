db.createCollection("jstests_capped", {capped:true, size:30000});

assert.eq( 2, db.system.indexes.find( {ns:"test.jstests_capped"} ).count(), "expected two indexes for new capped collection (hidden pk and _id)" );
t = db.jstests_capped;

t.save({x:1});
t.save({x:2});

assert( t.find().sort({$natural:1})[0].x == 1 , "expected obj.x==1");
assert( t.find().sort({$natural:-1})[0].x == 2, "expected obj.x == 2");
