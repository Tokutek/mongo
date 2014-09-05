
// test MX-1262
db.p_test.drop();
db.createCollection("p_test", {partitioned: 1, primaryKey: {a: 1, _id: 1}});
db.p_test.stats();
db.p_test.ensureIndex({u: 1, c: 1});
db.p_test.insert({a: 1, u: 2, t: 3});
db.p_test.find();
db.p_test.stats();
db.p_test.ensureIndex({u: 1, t: 1});
db.p_test.stats();
x = db.p_test.dropIndex("u_1_c_1"); //before fix to MX-1262, this would crash
assert.eq(4, x.nIndexesWas);
db.p_test.stats();
db.p_test.drop();

