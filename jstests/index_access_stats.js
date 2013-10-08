// Simple test for index access stats
t = db.indexaccessstats;
t.drop();
t.insert({ a: 1, b: 1 });

function expect(idxName, o) {
    indexes = t.stats()["indexDetails"];
    st = indexes.filter( function(o) { return o["name"] == idxName } )[0];
    assert.eq(o['q'] || 0, st['queries']);
    assert.eq(o['n'] || 0, st['nscanned']);
    assert.eq(o['o'] || 0, st['nscannedObjects']);
    assert.eq(o['i'] || 0, st['inserts']);
    assert.eq(o['d'] || 0, st['deletes']);
}

expect("_id_", { q: 0, i: 1  });

// We access the _id index to build secondary indexes,
// so the counter for queries and nscanned go up.
t.ensureIndex({ a: 1 });
expect("_id_", { q: 1, i: 1, n: 1 });
expect("a_1", { q: 0, i: 0 });
t.ensureIndex({ a: 1, b: 1 });
expect("_id_", { q: 2, i: 1, n: 2 });
expect("a_1", { q: 0, i: 0 });
expect("a_1_b_1", { q: 0, i: 0 });

t.find({ c: 1 }).itcount();
expect("_id_", { q: 3, i: 1, n: 3 });
expect("a_1", { q: 0, i: 0 });
expect("a_1_b_1", { q: 0, i: 0 });
t.find({ a: 1, b: 5 }).hint({ a: 1, b: 1 }).itcount();
expect("_id_", { q: 3, i: 1, n: 3 });
expect("a_1", { q: 0, i: 0 });
expect("a_1_b_1", { q: 1, i: 0, n: 0 }); // nscanned 0, query didn't match anything
t.find({ a: 1 }).hint({ a: 1, b: 1 }).itcount();
expect("_id_", { q: 3, i: 1, n: 3 });
expect("a_1", { q: 0, i: 0 });
expect("a_1_b_1", { q: 2, i: 0, n: 1, o: 1 }); // nscanned 1, query matched single document
t.find({ a: 1 }).hint({ a: 1 }).itcount();
expect("_id_", { q: 3, i: 1, n: 3 });
expect("a_1", { q: 1, i: 0, n: 1, o: 1 }); // nscanned 1, query matched single document
expect("a_1_b_1", { q: 2, i: 0, n: 1, o: 1 });

// Should insert into all indexes
t.insert({ a: 1, b: 1 });
expect("_id_", { q: 3, i: 2, n: 3 });
expect("a_1", { q: 1, i: 1, n: 1, o: 1 });
expect("a_1_b_1", { q: 2, i: 1, n: 1, o: 1 });

// Should just query the _id index
t.ensureIndex({ b: 1 }, { sparse: true });
expect("_id_", { q: 4, i: 2, n: 5 }); // scanned two more
expect("a_1", { q: 1, i: 1, n: 1, o: 1 });
expect("a_1_b_1", { q: 2, i: 1, n: 1, o: 1 });

// Should insert into non-sparse indexes
t.insert({ a: 1 });
expect("_id_", { q: 4, i: 3, n: 5 });
expect("a_1", { q: 1, i: 2, n: 1, o: 1 });
expect("a_1_b_1", { q: 2, i: 2, n: 1, o: 1 });
expect("b_1", { q: 0, i: 0, n: 0, o: 0 });

// Should insert into all indexes
t.insert({ b: 1 });
expect("_id_", { q: 4, i: 4, n: 5 });
expect("a_1", { q: 1, i: 3, n: 1, o: 1 });
expect("a_1_b_1", { q: 2, i: 3, n: 1, o: 1 });
expect("b_1", { q: 0, i: 1, n: 0, o: 0 });

// Should delete four docs from non-sparse indexes, three from sparse
t.remove({});
expect("_id_", { q: 5, i: 4, n: 9, d: 4 }); // assume we used a table scan
expect("a_1", { q: 1, i: 3, n: 1, o: 1, d: 4 }); // d > i because index built after first insert
expect("a_1_b_1", { q: 2, i: 3, n: 1, o: 1, d: 4 }); // d > i because index built after first insert
expect("b_1", { q: 0, i: 1, n: 0, o: 0, d: 3 });

// Should add a query for each index, but keep nscanned the same
// since nothing will match.
t.find({ _id: 1 }).hint({ _id: 1 }).itcount();
t.find({ a: 1 }).hint({ a: 1 }).itcount();
t.find({ a: 1, b: 1 }).hint({ a: 1, b: 1 }).itcount();
t.find({ b: 1 }).hint({ b: 1 }).itcount();
expect("_id_", { q: 6, i: 4, n: 9, d: 4 }); // assume we used a table scan
expect("a_1", { q: 2, i: 3, n: 1, o: 1, d: 4 }); // d > i because index built after first insert
expect("a_1_b_1", { q: 3, i: 3, n: 1, o: 1, d: 4 }); // d > i because index built after first insert
expect("b_1", { q: 1, i: 1, n: 0, o: 0, d: 3 });

t.drop();
