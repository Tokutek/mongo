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
        conn.getDB("test").foo.insert({_id : i, state : 0});
    }
}

preloadMoreData = function(conn) {
    for (var i = 0; i < 10; i++) {
        conn.getDB("test").foo.update({_id : i} , {$inc : {state : 1} });
    }
    for (var i = 0; i < 10; i++) {
        conn.getDB("test").foo.update({_id : i} , {$inc : {state : 1} });
    }
    for (var i = 0; i < 10; i++) {
        conn.getDB("test").foo.update({_id : i} , {$inc : {state : 1} });
    }
};

doSetup = function(conn) {
    // this should put us in the rollback state of RB_STARTING, which we cannot recover from
    x = conn.getDB("local").oplog.rs.find().sort({_id : -1}).next();
    lastGTID = x["_id"];
    conn.getDB("local").replInfo.insert({_id : "rollbackStatus", state : NumberInt(2), info : "supposedly removed docs"});
    // now let's figure out what we are actually removing
    // need to do two things, remove docs we care about,
    // and put them in local.rollback.docs
    conn.getDB("test").foo.remove({_id : 1});
    x = conn.getDB("local").oplog.rs.find().sort({_id : -1}).next();
    conn.getDB("local").oplog.rs.remove({_id : x["_id"]});

    conn.getDB("test").foo.remove({_id : 3});
    x = conn.getDB("local").oplog.rs.find().sort({_id : -1}).next();
    conn.getDB("local").oplog.rs.remove({_id : x["_id"]});

    conn.getDB("test").foo.remove({_id : 5});
    x = conn.getDB("local").oplog.rs.find().sort({_id : -1}).next();
    conn.getDB("local").oplog.rs.remove({_id : x["_id"]});

    // now insert them into local.rollback.docs
    conn.getDB("local").rollback.docs.insert({_id : { ns : "test.foo", pk : { "" : 1} }});
    conn.getDB("local").rollback.docs.insert({_id : { ns : "test.foo", pk : { "" : 3} }});
    conn.getDB("local").rollback.docs.insert({_id : { ns : "test.foo", pk : { "" : 5} }});
};

doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doSetup, false );

