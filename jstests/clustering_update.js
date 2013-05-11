var t = db.foo;

t.drop();
t.ensureIndex({x:1}, {clustering: true});

t.insert({x:1, y:1});
assert.eq(1, t.findOne().y);
assert.eq(1, t.findOne({x:1}).y);

o = t.findOne();
o.y = 2;
t.update({_id: o._id}, o);
db.getLastError();

assert.eq(2, t.findOne().y);
assert.eq(2, t.findOne({x:1}).y);
