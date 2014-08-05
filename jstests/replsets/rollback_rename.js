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

doRename = function(conn) {
    conn.getDB("test").foo.renameCollection("bar");
};

doRenameForwardPhase = function(conn) {
    preloadMoreData(conn);
    doRename(conn);
};

doDropIndexForwardPhase = function(conn) {
    preloadMoreData(conn);
    conn.getDB("test").foo.ensureIndex({a:1});
    assert.commandWorked(conn.getDB("test").foo.dropIndex({a:1}));
}

doDropForwardPhase = function(conn) {
    preloadMoreData(conn);
    conn.getDB("test").foo.ensureIndex({a:1});
    assert.commandWorked(conn.getDB("test").foo.dropIndex({a:1}));
    assert(conn.getDB("test").foo.drop());
}

doCreateForwardPhase = function(conn) {
    preloadMoreData(conn);
    conn.getDB("test").foo.ensureIndex({a:1});
    assert.commandWorked(conn.getDB("test").foo.dropIndex({a:1}));
    assert(conn.getDB("test").foo.drop());
    assert.commandWorked(conn.getDB("test").createCollection("foo"));
}

doCreateForwardPhase2 = function(conn) {
    preloadMoreData(conn);
    conn.getDB("test").foo.ensureIndex({a:1});
    assert.commandWorked(conn.getDB("test").foo.dropIndex({a:1}));
    assert(conn.getDB("test").foo.drop());
    assert.commandWorked(conn.getDB("test").createCollection("bar"));
    assert.commandWorked(conn.getDB("test").createCollection("zzz"));
}

doSimpleUpdate = function(conn) {
    conn.getDB("test").foo.update({_id : 5}, {$set : {state : "Shall go fatal!"}});
};

doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doRename, true );
doRollbackTest( 15, 1000000, 31000, preloadData, doRenameForwardPhase, doSimpleUpdate, true );
doRollbackTest( 15, 1000000, 31000, preloadData, doDropIndexForwardPhase, doSimpleUpdate, false );
doRollbackTest( 15, 1000000, 31000, preloadData, doDropForwardPhase, doSimpleUpdate, false );
doRollbackTest( 15, 1000000, 31000, preloadData, doCreateForwardPhase, doSimpleUpdate, true );
doRollbackTest( 15, 1000000, 31000, preloadData, doCreateForwardPhase2, doSimpleUpdate, false );

