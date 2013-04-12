t = db.capped_multi;
t.drop()

db.createCollection("capped_multi" , { capped: true, size: 1024 }); 
assert.eq( 0, t.count() )
t.insert( { _id: 0 } )
assert.eq( 1, t.count() )
size = t.stats().cappedSizeCurrent
assert( size > 0 )

// make sure a failed multi insert rolls back properly
// this fails because of a duplicate key error for _id : 0
t.insert( [ { _id: 1 } , { _id: 2 }, { _id: 0 }, { _id: 4 } ] )
assert( db.getLastError() )
st = t.stats()
assert( st.cappedSizeCurrent == size )
assert( st.cappedCount == 1 )

// make sure truncated documents are restored on abort
t.drop()
db.createCollection("capped_multi" , { capped: true, size:1024, max:3 })
t.insert( { _id: 0, a: "a" } ) // ...single insert for one doc
t.insert( [ { _id: 1, b: "bb" }, { _id: 2, c: "ccc" } ] ) // ...multi insert to make it 3, shouldn't matter though
st = t.stats()
oldSize = st.cappedSizeCurrent
assert( oldSize > 0 ) // sanity
assert( st.cappedCount == 3 ) // this is the max, we haven't gone over.

function verify(t) {
    st = t.stats()
    assert( st.cappedSizeCurrent == oldSize ) // should not have changed
    assert( st.cappedCount == 3 ) // should not have changed
    assert.eq( 1, t.find({ _id: 0 }).count() )
    assert.eq( 1, t.find({ _id: 1 }).count() )
    assert.eq( 1, t.find({ _id: 2 }).count() )
    assert.eq( "a", t.findOne({ _id: 0 }).a )
    assert.eq( "bb", t.findOne({ _id: 1 }).b )
    assert.eq( "ccc", t.findOne({ _id: 2 }).c )
    assert.eq( 3, t.count() )
}

// push out a single document before failure via single insert
t.insert( { _id: 2, z:1, b:1, a:"hello" } )
assert( db.getLastError() )
verify(t)

// push out a single document before failure via multi insert
t.insert( [ { _id: 4 } , { _id: 2 } ] )
assert( db.getLastError() )
verify(t)

// push out all 3 documents before failure via multi insert,
// where the duplicate _id is equal to the first document (4)
// this tests that we properly unique check before trimming.
t.insert( [ { _id: 4 } , { _id: 5 }, { _id: 6 }, { _id: 4 } ] )
assert( db.getLastError() )
verify(t)

// push out all 3 documents and insert a previously-duplicate key before failure via multi insert
t.insert( [ { _id: 4 } , { _id: 5 }, { _id: 6 }, { _id: 7 }, { _id: 2 }, { _id: 7 } ] )
assert( db.getLastError() )
verify(t)
