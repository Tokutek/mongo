// touching of a collection
// that does not exist works as expected
f = "jstests_6072";
db.jstests_6072.drop();

g = db.runCommand({touch:f, data:true, index:true});
assert.eq(0, g.ok);
assert.eq("ns not found", g.errmsg);

// now that collection is created, make sure touch succeeds
db.jstests_6072.insert({a:1});
g = db.runCommand({touch:f, data:true, index:true});
assert.eq(1, g.ok);

db.jstests_6072.drop();
