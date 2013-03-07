f = db.jstests_6192;
f.drop();

// simple findAndModifyTest with 'new' set to true
// verify that the row returned is the new value
f.insert({_id:1, a:1});
g = f.findAndModify({update: {$inc: {a:1}}, 'new': true});
assert.eq(2, g.a);

f.drop();
