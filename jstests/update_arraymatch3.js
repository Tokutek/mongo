
t = db.update_arraymatch3;
t.drop();

o = { _id : 1 , 
      title : "ABC",
      comments : [ { "by" : "joe", "votes" : 3 }, 
                   { "by" : "jane", "votes" : 7 } 
                 ] 
    }

t.save( o );
assert.eq( o , t.findOne() , "A1" );

t.update( {'comments.by':'joe'}, {$inc:{'comments.$.votes':1}}, false, true )
o.comments[0].votes++;
o2 = t.findOne()
assert.eq( o._id , o2._id , "A2 _id" );
assert.eq( o.title , o2.title , "A2 title" );
assert.eq( o.comments , o2.comments , "A2 comments" );
