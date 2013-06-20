// Test that _id projections are always index-only.

t = db.idprojection;
t.drop();
t.ensureIndex({ a: 1 });
for (i = 0; i < 10; i++) {
    t.insert({ _id: i, a: i, b: i, c: i});
}

function runTest(indexPattern, includeID) {
    t.dropIndexes();
    t.ensureIndex(indexPattern);
    assert(t.find({ a: 5 }, { a: 1, _id: includeID }).explain().indexOnly);
    assert(t.find({ a: 5, x: 100 }, { a: 1, _id: includeID }).explain().indexOnly);
    assert(t.find({ a: { $gt: 9 } }, { a: 1, _id: includeID }).explain().indexOnly);
    assert(t.find({ a: { $gt: 9 }, y : { $in: [ 1, 2, 1000 ] } }, { a: 1, _id: includeID }).explain().indexOnly);
    assert(t.find({ a: { $lt: 2 } }, { a: 1, _id: includeID }).explain().indexOnly);
    assert(t.find({ a: { $in: [ 1, 2, 5, 500] } }, { a: 1, _id: includeID }).explain().indexOnly);
    assert(t.find({ a: 1500 }, { _id: 1 }).explain().indexOnly);
    if (includeID) {
        assert(t.find({ a: { $in: [ 1, 2, 5, 500] } }, { _id: includeID }).explain().indexOnly);
        assert(t.find({ a: { $lte: 1500 } }, { _id: includeID }).explain().indexOnly);
    } else {
        // if we're not including the id, then the empty projection can never be index only
        assert(!t.find({ a: { $in: [ 1, 2, 5, 500] } }, { _id: includeID }).explain().indexOnly);
        assert(!t.find({ a: { $lte: 1500 } }, { _id: includeID }).explain().indexOnly);
    }
}

// with includeID - should always be indexOnly
runTest({ a: 1 }, 1);
runTest({ a: 1, _id: 1 }, 1);
runTest({ a: 1, x: 1 }, 1);
runTest({ a: 1, x: 1, _id: 1 }, 1);
// and without includeID - should still be indexOnly
runTest({ a: 1 }, 0);
runTest({ a: 1, _id: 1 }, 0);
runTest({ a: 1, x: 1 }, 0);
runTest({ a: 1, x: 1, _id: 1 }, 0);
