t = db.inc3;

function assertObjsEq(a, b) {
    for (var i in a) {
        assert.eq(a[i], b[i], i);
    }
    for (var i in b) {
        assert.eq(b[i], a[i], i);
    }
}

t.drop();
t.save( { _id : 1 , z : 1 , a : 1 } );
t.update( {} , { $inc : { z : 1 , a : 1 } } );
t.update( {} , { $inc : { a : 1 , z : 1 } } );
assertObjsEq({_id:1, z:3, a:3}, t.findOne());

t.drop();
t.save( { _id : 1 , a : 1 , z : 1 } );
t.update( {} , { $inc : { z : 1 , a : 1 } } );
t.update( {} , { $inc : { a : 1 , z : 1 } } );
assertObjsEq({_id:1, z:3, a:3}, t.findOne());

