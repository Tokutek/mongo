// simple test that if we are in state of having applied snapshot of docs,
// we can run rollback, GTIDSet is essentially empty

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

var pointA;
var pointB;

preloadMoreData = function(conn) {
    for (var i = 0; i < 10; i++) {
        conn.getDB("test").foo.update({_id : i} , {$inc : {state : 1} });
    }
    pointA = conn.getDB("local").oplog.rs.find().sort({_id : -1}).next();
    for (var i = 0; i < 10; i++) {
        conn.getDB("test").foo.update({_id : i} , {$inc : {state : 1} });
    }
    pointB = conn.getDB("local").oplog.rs.find().sort({_id : -1}).next();
    for (var i = 0; i < 10; i++) {
        conn.getDB("test").foo.update({_id : i} , {$inc : {state : 1} });
    }
};

var shouldFail = false;

doSetup = function(conn) {
    // this should put us in the rollback state of RB_STARTING, which we cannot recover from
    var hash;
    if (shouldFail) {
        hash = 1
    }
    else {
        hash = pointB["h"]
    }
    conn.getDB("local").replInfo.insert({
        _id : "rollbackStatus", state : NumberInt(3),
        info : "supposedly applied docs",
        lastGTID : pointA["_id"],
        lastHash : pointA["h"],
        lastGTIDAfterSnapshotDone : pointB["_id"],
        lastTSAfterSnapshotDone : pointB["ts"],
        lastHashAfterSnapshotDone : hash
        });
    // now let's figure out what we are actually removing
    // need to do two things, remove docs we care about,
    // and put them in local.rollback.docs
    conn.getDB("test").foo.update({_id : 1} , {$inc : {state : 1} });
    x = conn.getDB("local").oplog.rs.find().sort({_id : -1}).next();
    conn.getDB("local").oplog.rs.remove({_id : x["_id"]});

    conn.getDB("test").foo.update({_id : 3} , {$inc : {state : 1} });
    x = conn.getDB("local").oplog.rs.find().sort({_id : -1}).next();
    conn.getDB("local").oplog.rs.remove({_id : x["_id"]});

    conn.getDB("test").foo.update({_id : 5} , {$inc : {state : 1} });
    x = conn.getDB("local").oplog.rs.find().sort({_id : -1}).next();
    conn.getDB("local").oplog.rs.remove({_id : x["_id"]});

    // now insert them into local.rollback.docs
    conn.getDB("local").rollback.docs.insert({_id : { ns : "test.foo", pk : { "" : 1} }});
    conn.getDB("local").rollback.docs.insert({_id : { ns : "test.foo", pk : { "" : 3} }});
    conn.getDB("local").rollback.docs.insert({_id : { ns : "test.foo", pk : { "" : 5} }});
    // make GTIDSet
    conn.getDB("local").rollback.gtidset.insert({_id : "minUnapplied", gtid : GTID(100,0)});
};

doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doSetup, false );
shouldFail = true;
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doSetup, true );

