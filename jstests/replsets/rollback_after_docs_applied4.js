// simple test that if we are in state of having applied snapshot of docs,
// we can run rollback, we resume from after lastGTID, but before lastGTIDAfterSnapshot

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
var fail;

preloadMoreData = function(conn) {
    for (var i = 0; i < 10; i++) {
        conn.getDB("test").foo.update({_id : i} , {$inc : {state : 1} });
    }
    pointA = conn.getDB("local").oplog.rs.find().sort({_id : -1}).next();
    for (var i = 0; i < 10; i++) {
        conn.getDB("test").foo.update({_id : i} , {$inc : {state : 1} });
    }
    if (fail) {
        conn.getDB("test").foo.renameCollection("bar");
    }
    else {
        conn.getDB("test").foo.drop();
        conn.getDB("test").createCollection("foo");
    }
    pointB = conn.getDB("local").oplog.rs.find().sort({_id : -1}).next();
};

doSetup = function(conn) {
    // this test verifies that if we start in the after snapshot phase,
    // and we have gotten to lastGTID, but not yet to lastGTIDAfterSnapshotDone,
    // that if we encounter a rename, we will go fatal, but if we encounter a create, we won't
    conn.getDB("local").replInfo.insert({
        _id : "rollbackStatus", state : NumberInt(3),
        info : "supposedly applied docs",
        lastGTID : GTID(1,1),
        lastHash : 1,
        lastGTIDAfterSnapshotDone : pointB["_id"],
        lastTSAfterSnapshotDone : pointB["ts"],
        lastHashAfterSnapshotDone : pointB["h"]
        });
    // now let's figure out what we are actually removing
    // need to do two things, remove docs we care about,
    // and put them in local.rollback.docs

    // now insert them into local.rollback.docs
    conn.getDB("local").rollback.docs.insert({_id : { ns : "test.foo", pk : { "" : 1} }});
    conn.getDB("local").rollback.docs.insert({_id : { ns : "test.foo", pk : { "" : 3} }});
    conn.getDB("local").rollback.docs.insert({_id : { ns : "test.foo", pk : { "" : 5} }});
    // make GTIDSet
    conn.getDB("local").rollback.gtidset.insert({_id : "minUnapplied", gtid : GTID(1,0)});
    conn.getDB("local").createCollection("rollback.opdata"); // dummy to get test passing
};

fail = true;
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doSetup, true );
fail = false;
doRollbackTest( 15, 1000000, 31000, preloadData, preloadMoreData, doSetup, false );

