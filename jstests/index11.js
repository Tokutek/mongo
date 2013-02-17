// XXX: TokuDB can handle these big keys, so instead this tests verifies
// we can find the document even if its index key is large.
// Reindex w/ field too large to index

coll = db.jstests_index11;
coll.drop();

var str = "xxxxxxxxxxxxxxxx";
str = str + str;
str = str + str;
str = str + str;
str = str + str;
str = str + str;
str = str + str;
str = str + str;
str = str + str;
str = str + 'q';

coll.insert({ k: 'a', v: str });

assert.eq(0, coll.find({ "k": "x" }).count(), "expected zero keys 1");

coll.ensureIndex({"k": 1, "v": 1});
coll.insert({ k: "x", v: str });

assert.eq(1, coll.find({"k": "x"}).count(), "B"); // SERVER-1716 // XXX TokuDB this works for us

coll.dropIndexes();
coll.ensureIndex({"k": 1, "v": 1});

assert.eq(1, coll.find({ "k": "x" }).count(), "expected zero keys 2");
