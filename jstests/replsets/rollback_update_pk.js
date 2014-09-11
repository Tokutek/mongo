// simple rollback tests of individual write operations
// nothing tricky in the go-forward phase

var filename;
//if (TestData.testDir !== undefined) {
//    load(TestData.testDir + "/replsets/_rollback_helpers.js");
//} else {
    load('jstests/replsets/_rollback_helpers.js');
//}

preloadData = function(conn) {
    assert.commandWorked(conn.getDB("test").createCollection("foo", {primaryKey : {a : 1, _id: 1}}));
    // do some insertions
    for (var i = 0; i < 10; i++) {
        conn.getDB("test").foo.insert({a : i, _id : i, state : 0});
    }
}

preloadMoreData = function(conn) {
    for (var i = 10; i < 13; i++) {
        conn.getDB("test").foo.insert({a : i, _id: i, state : 100});
    }
    // with this one, the pre-image ought to be in the docs map, but not the post image
    conn.getDB("test").foo.update({a : 5, _id : 5}, {$inc : {a : 30}});
    for (var i = 13; i < 16; i++) {
        conn.getDB("test").foo.insert({a : i, _id : i, state : 100});
    }
    conn.getDB("test").foo.update({a : 15, _id : 15}, {$set : {b : 3}});
    for (var i = 16; i < 20; i++) {
        conn.getDB("test").foo.insert({a : i, _id : i, state : 100});
    }
    conn.getDB("test").foo.insert({a : 50, _id : 50, state : 102});
    conn.getDB("test").foo.insert({a : 52, _id : 2, state : 102});
};

doUpdate = function(conn) {
    conn.getDB("test").foo.update({a : 5, _id : 5}, {$inc : {a : 10}});
    conn.getDB("test").foo.insert({a : 50, _id : 50, state : 10});
    conn.getDB("test").foo.update({a : 50, _id : 50}, {$inc : {a : 1}});
    conn.getDB("test").foo.insert({a : 53, _id : 200, state : 101});
    conn.getDB("test").foo.update({a : 53, _id : 200}, {$inc : {state : 1}});
    
};

doFullUpdate = function(conn) {
    conn.getDB("test").foo.update({a : 5, _id : 5}, {a : 15, _id: 5, state : 1000});
};

// have a test case where during the forward phase of rollback, we encounter
// a document whose PK changes, we want to test these 4 scenarios:
// - neither old or new is in docs map
// - old is in docs map, but new is not
// - new is in docs map, but old is not
// - both are in docs map
preloadMoreData2 = function(conn) {
    // first insert more data
    for (var i = 10; i < 100; i++) {
        conn.getDB("test").foo.insert({a : 2*i, _id: 2*i, state : i});
    }

    // assuming doUpdate, written above, is what was done for rollback

    // this update should have 
    // with this one, the pre-image ought to be in the docs map, but not the post image
    conn.getDB("test").foo.update({a : 5, _id : 5}, {$inc : {a : 30}});
    // with this one, both pre-image and post-image ought to be there
    conn.getDB("test").foo.update({a : 50, _id : 50}, {$inc : {a : 1, state : 2}});
    // with this one, we expect (53,200) to be in the docs map, but not (52,200)
    conn.getDB("test").foo.insert({a : 52, _id : 200, state : 11});
    conn.getDB("test").foo.update({a : 52, _id : 200}, {$inc : {a : 1, state : 2}});
    conn.getDB("test").foo.insert({a : 1000, _id : 1000, state : 101});
    // expecting neither of these in the docs map
    conn.getDB("test").foo.update({a : 1000, _id : 1000}, {$inc : {a : 1, state : 2}});
};

doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData2, doUpdate, false );
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doUpdate, false );
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doFullUpdate, false );

