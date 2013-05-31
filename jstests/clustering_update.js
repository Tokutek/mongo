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

assert.eq(2, t.findOne().y, "y did not get set to 2, using the _id index");
assert.eq(2, t.findOne({x:1}).y, "y did not get set to 2, using the clustering index on x");

t.drop();
t.ensureIndex({x:1}, {clustering: true, unique: true});

t.insert({x:1, y:1});
t.insert({x:2, y:2});
assert.eq(2, t.find().itcount());
assert.eq(1, t.findOne({x:1}).y);
assert.eq(2, t.findOne({x:2}).y);

o = t.findOne({x:1});
o.x = 2;
t.update({_id: o._id}, o);
assert.neq(null, db.getLastError());
assert.eq(2, t.find().itcount());
assert.eq(1, t.findOne({x:1}).y);
assert.eq(2, t.findOne({x:2}).y);
