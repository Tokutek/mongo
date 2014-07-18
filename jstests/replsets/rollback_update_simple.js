// simple rollback tests of individual write operations
// nothing tricky in the go-forward phase

var filename;
//if (TestData.testDir !== undefined) {
//    load(TestData.testDir + "/replsets/_rollback_helpers.js");
//} else {
    load('jstests/replsets/_rollback_helpers.js');
//}

preloadData = function(conn) {
    conn.getDB("test").createCollection("foo");
    conn.getDB("test").foo.ensureIndex({a:1});
    // do some insertions
    for (var i = 0; i < 10; i++) {
        conn.getDB("test").foo.insert({_id : i, state : 0});
    }
}

preloadMoreData = function(conn) {
    for (var i = 10; i < 13; i++) {
        conn.getDB("test").foo.insert({_id : i, state : 100});
    }
    conn.getDB("test").foo.update({_id : 5}, {$inc : {state : 3}});
    for (var i = 13; i < 16; i++) {
        conn.getDB("test").foo.insert({_id : i, state : 100});
    }
    conn.getDB("test").foo.update({_id : 5}, {$inc : {state : 3}});
    for (var i = 16; i < 20; i++) {
        conn.getDB("test").foo.insert({_id : i, state : 100});
    }
    conn.getDB("test").foo.update({_id : 5}, {$inc : {state : 3}});
};

doUpdate = function(conn) {
    conn.getDB("test").foo.update({_id : 5}, {$inc : {state : 1}});
};

doFullUpdate = function(conn) {
    conn.getDB("test").foo.update({_id : 5}, {state : 1000});
};

doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doUpdate, false );
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doFullUpdate, false );

