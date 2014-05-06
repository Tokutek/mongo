
t = db.basic6;

t.findOne();
t.a.findOne();

assert.eq( db.getName() + ".basic6" , t.toString() );
assert.eq( db.getName() + ".basic6.a" , t.a.toString() );
