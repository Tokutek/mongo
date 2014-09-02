// test that verifies if local.rollback.opdata stores the correct information

var filename;
//if (TestData.testDir !== undefined) {
//    load(TestData.testDir + "/replsets/_rollback_helpers.js");
//} else {
    load('jstests/replsets/_rollback_helpers.js');
//}

var firstGTID;
var firstOp1;
var firstOp2;
var secondGTID;
var secondOp;
var thirdGTID;
var thirdOp1;
var thirdOp2;

var generateFakeOpData;

preloadData = function(conn) {
    conn.getDB("test").createCollection("foo");
    conn.getDB("test").foo.ensureIndex({a:1});
    // do some insertions
    for (var i = 0; i < 10; i++) {
        conn.getDB("test").foo.insert({_id : i, state : "before rollback"});
    }
}

function doItemsToRollBack(conn) {
    var db = conn.getDB("test");
    var localdb = conn.getDB("local");
    // multiple inserts
    db.foo.insert([{_id : 1000, a : 1000},{_id : 2000, a : 2000}]);
    var x = localdb.oplog.rs.find().sort({$natural : -1}).next();
    print("********* op entry: ***********");
    printjson(x);
    firstGTID = x._id;
    firstOp1 = x["ops"][0];
    firstOp2 = x["ops"][1];
    db.foo.update({_id : 6}, {$set: {a : 20}});
    x = localdb.oplog.rs.find().sort({$natural : -1}).next();
    print("********* op entry: ***********");
    printjson(x);
    secondGTID = x._id;
    assert(firstGTID != secondGTID);
    secondOp = x["ops"][0];
    db.runCommand("beginTransaction");
    db.foo.remove({_id : 8});
    db.foo.insert({_id : 60});
    db.runCommand("commitTransaction");
    var x = localdb.oplog.rs.find().sort({$natural : -1}).next();
    assert(thirdGTID != secondGTID);
    print("********* op entry: ***********");
    printjson(x);
    thirdGTID = x._id;
    thirdOp1 = x["ops"][0];
    thirdOp2 = x["ops"][1];

    if (generateFakeOpData) {
        // fake entry, all that matters is _id is something that's possible to have
        localdb.rollback.opdata.insert({ _id : {rid : 10, seq : 0}});
    }
}

checkResults = function(conn) {
    // post rollback, now let's make sure the data is saved in local.rollback.opdata
    var localdb = conn.getDB("local");
    var c = localdb.rollback.opdata.find().sort({$natural : -1});

    var expectedRid = 0;
    if (generateFakeOpData) {
        expectedRid = 11;
    }

    x = c.next();
    printjson(x);
    assert.eq(x["_id"]["rid"], expectedRid);
    assert.eq(x["_id"]["seq"], 4);
    assert.eq(x.gtid, firstGTID);
    assert(friendlyEqual(x.op, firstOp1));
    x = c.next();
    printjson(x);
    assert.eq(x["_id"]["rid"], expectedRid);
    assert.eq(x["_id"]["seq"], 3);
    assert(friendlyEqual(x.op, firstOp2));

    x = c.next();
    printjson(x);
    assert.eq(x["_id"]["rid"], expectedRid);
    assert.eq(x["_id"]["seq"], 2);
    assert.eq(x.gtid, secondGTID);
    assert(friendlyEqual(x.op, secondOp));

    x = c.next();
    printjson(x);
    assert.eq(x["_id"]["rid"], expectedRid);
    assert.eq(x["_id"]["seq"], 1);
    assert.eq(x.gtid, thirdGTID);
    assert(friendlyEqual(x.op, thirdOp1));
    x = c.next();
    printjson(x);
    assert.eq(x["_id"]["rid"], expectedRid);
    assert.eq(x["_id"]["seq"], 0);
    assert(friendlyEqual(x.op, thirdOp2));
}

preloadLotsMoreData = function(conn) {
    var res = conn.getDB("admin").runCommand({configureFailPoint: 'disableReplInfoThread', mode: 'alwaysOn'});
    assert.eq(1, res.ok, "could not disable repl info thread");

    for (var i = 10; i < 200; i++) {
        conn.getDB("test").foo.insert({_id : i, state : "after split"});
    }
};

// these test with oplog data that does not spill to oplog.refs
generateFakeOpData = true;
doRollbackTest( 15, 1000000, 31000, preloadData, preloadLotsMoreData, doItemsToRollBack, false, checkResults );
generateFakeOpData = false;
doRollbackTest( 15, 1000000, 31000, preloadData, preloadLotsMoreData, doItemsToRollBack, false, checkResults );

function doItemsToRollBack2(conn) {
    var db = conn.getDB("test");
    var localdb = conn.getDB("local");
    // multiple inserts
    db.foo.insert({_id : 1000, a : 10101});
    var x = localdb.oplog.rs.find().sort({$natural : -1}).next();
    print("********* op entry: ***********");
    printjson(x);
    firstGTID = x._id;
    var firstRef = x.ref;
    var y = localdb.oplog.refs.find({_id : {$gt : {oid : firstRef , seq : 0}}}).next();
    printjson(y);
    assert.eq(y["_id"]["oid"], firstRef);
    firstOp1 = y["ops"][0];
}

checkResults2 = function(conn) {
    // post rollback, now let's make sure the data is saved in local.rollback.opdata
    var localdb = conn.getDB("local");
    var c = localdb.rollback.opdata.find().sort({$natural : -1});

    x = c.next();
    printjson(x);
    assert.eq(x["_id"]["rid"], 0);
    assert.eq(x["_id"]["seq"], 0);
    assert.eq(x.gtid, firstGTID);
    assert(friendlyEqual(x.op, firstOp1));
}

// this is a very simple test on data that spills to oplog.refs
doRollbackTest( 15, 1, 31000, preloadData, preloadLotsMoreData, doItemsToRollBack2, false, checkResults2 );

