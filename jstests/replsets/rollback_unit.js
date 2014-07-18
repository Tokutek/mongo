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
        conn.getDB("test").foo.insert({_id : i, state : "before rollback"});
    }
}

preloadMoreData = function(conn) {
    for (var i = 0; i < 10; i++) {
        conn.getDB("test").foo.update({_id : i}, {state : 3});
        conn.getDB("test").foo.update({_id : i}, {$inc : {state : 3}});
    }
    for (var i = 10; i < 20; i++) {
        conn.getDB("test").foo.insert({_id : i, state : "after split"});
    }
    for (var i = 10; i < 20; i++) {
        conn.getDB("test").foo.update({_id : i}, {state : 3});
        conn.getDB("test").foo.update({_id : i}, {$inc : {state : 3}});
    }
    conn.getDB("test").foo.remove({_id : 18});
    conn.getDB("test").foo.remove({_id : 6});
};

forwardInsert = function(conn) {
    for (var i = 10; i < 20; i++) {
        conn.getDB("test").foo.insert({_id : i, state : "after split"});
    }
};

doInsert = function(conn) {
    conn.getDB("test").foo.insert({_id : 15, state : "God"});
    conn.getDB("test").foo.insert({_id : 18, state : "God"});
};

doDelete = function(conn) {
    conn.getDB("test").foo.remove({_id : 5});
    conn.getDB("test").foo.remove({_id : 6});
};

doUpdate = function(conn) {
    conn.getDB("test").foo.update({_id : 5}, {$set : {state : "Should be rolled back"}});
    conn.getDB("test").foo.update({_id : 6}, {$set : {state : "Should be rolled back"}});
};

doInsertAndUpdate = function(conn) {
    conn.getDB("test").foo.insert({_id : 15, state : "God"});
    conn.getDB("test").foo.update({_id : 15}, {$set : {state : "Should be rolled back"}});
    conn.getDB("test").foo.insert({_id : 18, state : "God"});
    conn.getDB("test").foo.update({_id : 18}, {$set : {state : "Should be rolled back"}});
};

doUpdateAndDelete = function(conn) {
    conn.getDB("test").foo.update({_id : 5}, {$set : {state : "Should be rolled back"}});
    conn.getDB("test").foo.remove({_id : 5});
    conn.getDB("test").foo.update({_id : 6}, {$set : {state : "Should be rolled back"}});
    conn.getDB("test").foo.remove({_id : 6});
};

doInsertAndUpdateAndDelete = function(conn) {
    conn.getDB("test").foo.insert({_id : 15, state : "God"});
    conn.getDB("test").foo.update({_id : 15}, {$set : {state : "Should be rolled back"}});
    conn.getDB("test").foo.remove({_id : 15});
    conn.getDB("test").foo.insert({_id : 18, state : "God"});
    conn.getDB("test").foo.update({_id : 18}, {$set : {state : "Should be rolled back"}});
    conn.getDB("test").foo.remove({_id : 18});
};


doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doInsert, false );
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doDelete, false );
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doUpdate, false );
doRollbackTest( 15, 1000000, 31000, preloadData, forwardInsert, doUpdate, false );
doRollbackTest( 15, 1000000, 31000, preloadData, forwardInsert, doInsertAndUpdate, false );
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doInsertAndUpdate, false );
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doUpdateAndDelete, false );
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doInsertAndUpdateAndDelete, false );

