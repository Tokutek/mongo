// This test verifies that operations should not require a forward phase
// do not force one

var filename;
//if (TestData.testDir !== undefined) {
//    load(TestData.testDir + "/replsets/_rollback_helpers.js");
//} else {
    load('jstests/replsets/_rollback_helpers.js');
//}

preloadData = function(conn) {
    assert.commandWorked(conn.getDB("test").createCollection("foo"));
    conn.getDB("test").foo.ensureIndex({a:1});
    assert.commandWorked(conn.getDB("test").createCollection("cappedFoo", {capped : true, size : 1024}));
    // do some insertions
    for (var i = 0; i < 10; i++) {
        conn.getDB("test").foo.insert({_id : i, state : "before rollback"});
        conn.getDB("test").cappedFoo.insert({_id : i, state : "before rollback"});
    }
}

preloadMoreData = function(conn) {
    for (var i = 10; i < 50; i++) {
        conn.getDB("test").foo.insert({_id : i, state : "after split"});
        conn.getDB("test").cappedFoo.insert({_id : i, state : "after split"});
    }
};

preloadCappedData = function(conn) {
    assert.commandWorked(conn.getDB("test").createCollection("cappedFoo", {capped : true, size : 1024}));
}

preloadMoreCappedData = function(conn) {
    for (var i = 100; i < 2048+100; i++) {
        conn.getDB("test").cappedFoo.insert({_id : i, state : "after split"});
    }
};

doCappedInserts = function(conn) {
    for (var i = 100; i < 1024+100; i++) {
        conn.getDB("test").cappedFoo.insert({_id : i, state : "roll me back"});
    }
}


doRename = function(conn) {
    conn.getDB("test").foo.renameCollection("bar");
};

doRenameForwardPhase = function(conn) {
    preloadMoreData(conn);
    doRename(conn);
};

doBackwardsOps = function(conn) {
    // insert
    conn.getDB("test").foo.insert({_id : 100, state : "blah"});
    // multi-insert
    conn.getDB("test").foo.insert([{_id : 101, state : "blah"},{_id : 121, state : "blah"}]);
    // delete
    conn.getDB("test").foo.remove({_id : 5});
    // capped insert
    conn.getDB("test").cappedFoo.insert({_id : 100, state : 12});
    // capped update
    conn.getDB("test").cappedFoo.update({_id : 100}, {$inc : {state : 1}});
    //conn.getDB("test").cappedFoo.update({_id : 1}, {state : 100});
    assert.commandWorked(conn.getDB("test").beginTransaction());
    conn.getDB("test").foo.insert({_id : 102, state : 1});
    conn.getDB("test").cappedFoo.insert({_id : 102, state : 1});
    conn.getDB("test").foo.remove({_id : 6});
    conn.getDB("test").cappedFoo.update({_id : 100}, {$inc : {state : 1}});
    assert.commandWorked(conn.getDB("test").commitTransaction());
};

doRollbackTest( 15, 1000000, 31000, preloadData, doRenameForwardPhase, doBackwardsOps, false );
doRollbackTest( 15, 1000000, 31000, preloadCappedData, preloadMoreCappedData, doCappedInserts, false );
// rollback_rename verifies that if there is an update, we go forward
