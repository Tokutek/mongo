// SERVER-8814: Test that only the system.indexes namespace can be used to build indexes.

function assertGLENotOK(status) {
    assert(status.ok && status.err !== null,
           "Expected not-OK status object; found " + tojson(status));
}

db = db.getSiblingDB("indexOtherNS");
db.dropDatabase();

db.foo.insert({a:1})
assert.eq(2, db.system.indexes.count());  // system.namespaces.$$_, foo.$_id_
assert.eq("BasicCursor", db.foo.find({a:1}).explain().cursor);

db.randomNS.system.indexes.insert({ns:"indexOtherNS.foo", key:{a:1}, name:"a_1"});
// In TokuMX it's fine to insert this document, but it should not cause an index build
assert.eq(db.getLastError());
// Assert that index didn't actually get built (but we did create a new collection with an _id index)
assert.eq(3, db.system.indexes.count());  // system.namespaces.$$_, foo.$_id_, randomNS.system.indexes.$_id_
assert.eq(null, db.system.namespaces.findOne({name : "indexOtherNS.foo.$a_1"}));
assert.eq("BasicCursor", db.foo.find({a:1}).explain().cursor);
