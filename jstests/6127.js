f = db.jstests_6127;
f.drop();

f.insert({a:1});
// assert that the name of the id index is "_id_"
assert.eq("_id__", f.getIndexes()[0].name);

f.drop();