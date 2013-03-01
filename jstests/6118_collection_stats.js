f = db.jstests_6118_collection_stats;
f.drop();

f.insert({a:1});
stats = f.stats();

// verify that basic stats work when we have just the id index
assert.eq(1, stats.nindexes);
assert.eq(1, stats.count);
assert.eq(0, stats.totalIndexSize);
assert.eq(0, stats.totalIndexStorageSize);
assert.eq(1, stats.indexDetails.length);
assert.eq("_id_", stats.indexDetails[0].name);
assert.eq(4194304, stats.indexDetails[0].pageSize);
assert.eq(65536, stats.indexDetails[0].readPageSize);
assert.eq("default", stats.indexDetails[0].compression);

// verify that after we add an index with a custom pageSize, readPageSize, and compression,
// we see those values in stats
f.ensureIndex({a:1}, {pageSize:1000000, compression:"zlib", readPageSize:32000});
stats = f.stats();
assert.eq(2, stats.nindexes);
assert.eq("a_1", stats.indexDetails[1].name);
assert.eq(1000000, stats.indexDetails[1].pageSize);
assert.eq(32000, stats.indexDetails[1].readPageSize);
assert.eq("zlib", stats.indexDetails[1].compression);

// basic test that scale works
kb_stats = f.stats(1000);
assert.eq(1000, kb_stats.indexDetails[1].pageSize);
assert.eq(32, kb_stats.indexDetails[1].readPageSize);

// test that count goes up when we insert another element
f.insert({b:345});
assert.eq(2, f.stats().count);
// this test verifies that even if an insertion
// does not have any fields that match an index,
// an insertion happens, and the count increments
assert.eq(2, f.stats().indexDetails[1].count);

f.drop();