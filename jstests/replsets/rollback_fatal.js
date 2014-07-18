// tests that various commands that show up in the backward scan cause us to go fatal.

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

doCreate = function(conn) {
    // dummy
    conn.getDB("test").foo.insert({_id : 15, state : "God"});
    conn.getDB("test").foo.insert({_id : 16, state : "God"});

    conn.getDB("test").createCollection("bar");

    // dummy
    conn.getDB("test").foo.insert({_id : 17, state : "God"});
    conn.getDB("test").foo.insert({_id : 18, state : "God"});
};

doAddIndex = function(conn) {
    // dummy
    conn.getDB("test").foo.insert({_id : 15, state : "God"});
    conn.getDB("test").foo.insert({_id : 16, state : "God"});

    conn.getDB("test").foo.ensureIndex({b:1});

    // dummy
    conn.getDB("test").foo.insert({_id : 17, state : "God"});
    conn.getDB("test").foo.insert({_id : 18, state : "God"});
};

doDrop = function(conn) {
    // dummy
    conn.getDB("test").foo.insert({_id : 15, state : "God"});
    conn.getDB("test").foo.insert({_id : 16, state : "God"});

    conn.getDB("test").foo.drop();

    // dummy
    conn.getDB("test").foo.insert({_id : 17, state : "God"});
    conn.getDB("test").foo.insert({_id : 18, state : "God"});
};

doDropIndex = function(conn) {
    // dummy
    conn.getDB("test").foo.insert({_id : 15, state : "God"});
    conn.getDB("test").foo.insert({_id : 16, state : "God"});

    assert.commandWorked(conn.getDB("test").foo.dropIndex({a:1}));

    // dummy
    conn.getDB("test").foo.insert({_id : 17, state : "God"});
    conn.getDB("test").foo.insert({_id : 18, state : "God"});
};

doDropDatabase = function(conn) {
    // dummy
    conn.getDB("test").foo.insert({_id : 15, state : "God"});
    conn.getDB("test").foo.insert({_id : 16, state : "God"});

    assert.commandWorked(conn.getDB("test").dropDatabase());

    // dummy
    conn.getDB("test").foo.insert({_id : 17, state : "God"});
    conn.getDB("test").foo.insert({_id : 18, state : "God"});
};

doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doCreate, true );
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doAddIndex, true );
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doDrop, true );
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doDropIndex, true );
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doDropDatabase, true );

