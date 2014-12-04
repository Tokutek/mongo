t = db.idhack2;
t.drop();

// Test that queries with _id in them return the correct results.
// Exercises the queryByIdHack path, which is supposed to always
// lookup by _id and match off the rest of the query

t.insert({ _id: 0, a: 0, b: 0 });
t.insert({ _id: 1, a: 1, b: 1 });

var checkQuery = function(expected, query) {
    assert.eq(expected, t.find(query).itcount());
};

for (val = 0; val < 2; val++) {
    checkQuery(1, { _id: val });
    checkQuery(1, { _id: { $in: [ val ] } });
    checkQuery(0, { _id: { x: val } });
    checkQuery(1, { _id: val, a: val });
    checkQuery(1, { _id: { $in: [ val ] }, a: val });
    checkQuery(0, { _id: { x: val }, a: val });
    checkQuery(1, { _id: val, b: val });
    checkQuery(0, { _id: val, b: val, a: val + 1 });
    checkQuery(0, { _id: { $in: [ val ] }, b: val, a: val + 1 });
    checkQuery(1, { a: val, b: val, _id: val });
    checkQuery(1, { b: val, a: val, _id: val });
    checkQuery(0, { a: val, b: val, _id: { x: val } });
    checkQuery(0, { b: val, a: val, _id: { x: val } });
    checkQuery(1, { a: val, b: val, _id: { $in: [ val ] } });
    checkQuery(1, { b: val, b: val, _id: { $in: [ val ] } });
}

// Check that array position projections still work with the query by pk hack
t.insert({ _id: 5, v: [ { a: 'cats' }, { a: 'dogs' } ] });

// with an _id in the query
assert.eq('cats', t.findOne({ _id: 5, 'v.a': 'cats' }, { 'v.$': 1 }).v[0].a);
assert.eq('dogs', t.findOne({ _id: 5, 'v.a': 'dogs' }, { 'v.$': 1 }).v[0].a);
// no _id in the query
assert.eq('cats', t.findOne({ 'v.a': 'cats' }, { 'v.$': 1 }).v[0].a);
assert.eq('dogs', t.findOne({ 'v.a': 'dogs' }, { 'v.$': 1 }).v[0].a);
