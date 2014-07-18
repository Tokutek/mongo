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
        conn.getDB("test").foo.insert({a : i, _id : 1, state : 0});
    }
}

preloadMoreData = function(conn) {
    for (var i = 10; i < 13; i++) {
        conn.getDB("test").foo.insert({a : i, _id: 1, state : 100});
    }
    conn.getDB("test").foo.update({a : 5, _id : 1}, {$inc : {a : 30}});
    for (var i = 13; i < 16; i++) {
        conn.getDB("test").foo.insert({_id : i, state : 100});
    }
    conn.getDB("test").foo.update({a : 15, _id : 1}, {$set : {b : 3}});
    for (var i = 16; i < 20; i++) {
        conn.getDB("test").foo.insert({_id : i, state : 100});
    }
    conn.getDB("test").foo.insert({a : 50, _id : 1, state : 102});
    conn.getDB("test").foo.insert({a : 52, _id : 2, state : 102});
};

doUpdate = function(conn) {
    conn.getDB("test").foo.update({a : 5, _id : 1}, {$inc : {a : 10}});
    conn.getDB("test").foo.insert({a : 50, _id : 1, state : 10});
    conn.getDB("test").foo.update({a : 50, _id : 1}, {$inc : {a : 1}});
    
};

doFullUpdate = function(conn) {
    conn.getDB("test").foo.update({a : 5, _id : 1}, {a : 15, _id: 1, state : 1000});
};

doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doUpdate, false );
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doFullUpdate, false );

