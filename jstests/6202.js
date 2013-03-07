// verify that we can create multiple indexes with the same key

t = db.jstests_6202
t.drop();

t.insert({a:1})
t.ensureIndex({a:1})
e = db.getLastErrorObj()
assert(e.err == null);
t.ensureIndex({a:1})
e = db.getLastErrorObj()
assert(e.err == null);

// printjson(t.getIndexes())

t.drop();
