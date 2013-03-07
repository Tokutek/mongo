// verify that unique index creation with duplicates fails with error code 11000

t = db.jstests_6200
t.drop();

t.insert({a:1})
t.insert({a:1})
t.ensureIndex({a:1},{unique:1})
e = db.getLastErrorObj()
printjson(e)
assert(e.code == 11000)
assert(/^E11000 /.test(e.err))

t.drop();
