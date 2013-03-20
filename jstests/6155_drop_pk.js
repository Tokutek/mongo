// test that dropping an id or $_ index does not work
f = db.jstests_6155;
f.drop();
f.insert({a:1});
g = f.dropIndex('_id_');
assert.eq(0, g.ok);
assert.eq("may not delete _id or $_ index", g.errmsg);
h = f.count()
assert.eq(1, h);
f.drop();
