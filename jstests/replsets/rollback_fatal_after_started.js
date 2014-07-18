// test that the existence of a rename properly makes us go fatal,
// whether it is encountered in forward phase or backward phase

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
    for (var i = 10; i < 20; i++) {
        conn.getDB("test").foo.insert({_id : i, state : "after split"});
    }
};

doSabotage = function(conn) {
    // this should put us in the rollback state of RB_STARTING, which we cannot recover from
    conn.getDB("local").replInfo.insert({_id : "rollbackStatus", state : NumberInt(1), info : "sabotage!!"});
};

doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doSabotage, true );

