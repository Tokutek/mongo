// test that we can create a collection with a user-defined key and then shard on a different key
// see #968

var st = new ShardingTest({ shards: 2 });
st.stopBalancer();

var testDB = st.s.getDB('test');
assert.commandWorked(testDB.adminCommand({ enableSharding: 'test' }));
assert.commandWorked(testDB.createCollection('user', {primaryKey: {a: 1, b: 1, _id: 1}}));
assert.commandWorked(testDB.adminCommand({ shardCollection: 'test.user', key: { x: 'hashed' }, clustering: false}));

var indexes = testDB.user.getIndexes();
assert.eq(indexes.length, 3);
assert.eq(indexes[0].name, 'primaryKey');
assert.eq(indexes[0].key, {a: 1, b: 1, _id: 1})
assert.eq(indexes[1].key, {_id: 1});
assert.eq(indexes[2].key, {x: 'hashed'});


st.stop();

