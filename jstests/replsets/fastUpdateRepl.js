
function dbs_match(a, b) {
    print("dbs_match, but not checking namespaces!");

    var c = a.getCollectionNames();
    for( var i in c ) {
        if (c[i] != "system.indexes") {
            print("checking " + c[i]);
            if( !friendlyEqual( a[c[i]].find().sort({_id:1}).toArray(), b[c[i]].find().sort({_id:1}).toArray() ) ) { 
                print("dbs_match: collections don't match " + c[i]);
                return false;
            }
        }
    }
    return true;
};


// check last entry is a full update with pre-image and post-image
verifyLastEntry = function (conn, expectedOp) {
    lastEntry = conn.getDB("local").oplog.rs.find().sort({_id:-1}).next();
    assert(lastEntry["ops"][0]["op"] == expectedOp);
}

assertUpdateSlow = function(wdb) {
    x = wdb.runCommand({getLastError : 1});
    assert.eq(x.ok, 1);
    assert.eq(x.updatedExisting, false);
    assert.eq(x.n, 0);
};

assertOplogEntrySlow = function(localdb) {
    y = localdb.oplog.rs.find().sort({$natural : -1}).next();
    assert.eq(y["ops"][0]["f"], 0); // means that update was not fast. 1 or 3 is fast
    assert.eq(y["ops"][0]["op"], "ur");
    assert.eq(undefined, y["ops"][0]["o"]);
}

assertUpdateFast = function(wdb, localdb) {
    x = wdb.runCommand({getLastError : 1});
    assert.eq(x.ok, 1);
    assert.eq(x.updatedExisting, true);
    assert.eq(x.n, 1);
    y = localdb.oplog.rs.find().sort({$natural : -1}).next();
    assert(y["ops"][0]["f"] > 0); // means that update was not fast. 1 or 3 is fast
    assert.eq(y["ops"][0]["op"], "ur");
    assert.eq(undefined, y["ops"][0]["o"]);
}

assertUpsertFast = function(wdb, localdb) {
    x = wdb.runCommand({getLastError : 1});
    assert.eq(x.ok, 1);
    assert.eq(x.updatedExisting, true);
    assert.eq(x.n, 1);
    y = localdb.oplog.rs.find().sort({$natural : -1}).next();
    assert(y["ops"][0]["f"] > 5); // means that update was not fast. 1 or 3 is fast
    assert.eq(y["ops"][0]["op"], "ur");
    assert.eq(undefined, y["ops"][0]["o"]);
}

getLastOp = function(localdb) {
    return localdb.oplog.rs.find().sort({$natural : -1}).next();
}

doIDPKTest = function (signal, txnLimit, startPort, secondaryHasIndex) {
    var num = 3;
    var host = getHostName();
    var name = "rollback_unit";
    var timeout = 60000;

    var replTest = new ReplSetTest( {name: name, nodes: num, startPort:startPort, txnMemLimit: txnLimit} );
    var conns = replTest.startSet();
    var port = replTest.ports;
    var config = {_id : name, members :
            [
             {_id:0, host : host+":"+port[0], priority:10 },
             {_id:1, host : host+":"+port[1]},
             {_id:2, host : host+":"+port[2], arbiterOnly : true},
            ],
             };

    replTest.initiate(config);
    replTest.awaitReplication();
    assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });


    // Make sure we have an arbiter
    assert.soon(function () {
        res = conns[2].getDB("admin").runCommand({ replSetGetStatus: 1 });
        return res.myState == 7;
    }, "Arbiter failed to initialize.");

    var wdb = conns[0].getDB("foo");
    // first basic test, that fast updates work only when enabled
    assert.commandWorked(wdb.createCollection("foo"));
    replTest.awaitReplication();
    
    if (secondaryHasIndex) {
        // make all the updates we do be non-indexed on primary
        // but indexed on secondary
        print("shutting down secondary");
        replTest.stop(1);
        print("restarting secondary without replset");
        replTest.restartWithoutReplset(1);
        conns[1].getDB("foo").foo.ensureIndex({a:1});
        assert(!conns[1].getDB("foo").getLastError());
        // bring it back up
        print("shutting down secondary");
        replTest.stop(1);
        print("restarting secondary in replset");
        replTest.restart(1);
        assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });
    }

    conns[0].setSlaveOk();
    conns[1].setSlaveOk();

    var swdb = conns[1].getDB("foo");
    var localdb = conns[0].getDB("local");
    // first basic test, that fast updates work only when enabled
    wdb.foo.insert({_id : 0, a : 0});
    //update of non-existent key should not work
    wdb.foo.update({_id : 1}, { $set : {a : 1}});
    assertUpdateSlow(wdb);
    // now let's do an update that works, verify that it is slow
    wdb.foo.update({_id : 0}, { $set : {a : 1}});
    assertOplogEntrySlow(localdb);

    // now let's turn on fast updates, do all the above things, and verify that they are indeed fast
    // this is a sanity check to verify that the setParameter works
    assert.commandWorked(wdb.adminCommand({ setParameter: 1, fastUpdates: true }));
    wdb.foo.update({_id : 0}, { $set : {a : 100}});
    assertUpdateFast(wdb, localdb);    
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");

    wdb.foo.update({_id : 1}, { $set : {a : 1}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");

    // now we've tested that the fastUpdates parameter works and that an update is fast only
    // if the flag is set. Now let's get to testing fast updates

    // these set of tests verify that operations that result in no changes are still fast and perform
    // message injections and oplog writes, and we verify that oplog entries are right

    // update with a query filter that does not match, even though the doc exists (_id : x, nonExistentField : 1)
    wdb.foo.insert({_id : 2, a : 2});
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({_id : 2, nonExistentField : 1}, { $set : {a : "abra"}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    x = wdb.foo.find({_id : 2}).next();
    assert.eq(x.a, 2); // make sure the doc is unchanged
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    assert(friendlyEqual({_id : 2, nonExistentField : 1}, lastOp["ops"][0]["q"])); // verify full query made it to oplog

    // update that does something illegal, like $inc a text field
    assert.eq(0, wdb.foo.count({_id : 3})); // sanity check
    wdb.foo.insert({_id : 3, a : "asdf"});
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({_id : 3}, { $inc : {a : 1}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    x = wdb.foo.find({_id : 3}).next();
    assert.eq(x.a, "asdf"); // make sure the doc is unchanged
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    assert(friendlyEqual({}, lastOp["ops"][0]["q"])); // verify no query made it to oplog

    // update on a non-existing document
    assert.eq(0, wdb.foo.count({_id : 4})); // sanity check
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({_id : 4}, { $inc : {a : 1}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    assert.eq(0, wdb.foo.count({_id : 4})); // sanity check that doc still doesn't exist
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    assert(friendlyEqual({}, lastOp["ops"][0]["q"])); // verify no query made it to oplog


    // these set of tests shows that fast updates work on non-expecting scenarios:

    // normal fast update with no additional query, query is empty in oplog
    assert.eq(0, wdb.foo.count({_id : 4})); // sanity check
    wdb.foo.insert({_id : 4, a : 2});
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({_id : 4}, { $inc : {a : 1}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    x = wdb.foo.find({_id : 4}).next();
    assert.eq(x.a, 3); // make sure the doc is properly changed
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    assert(friendlyEqual({}, lastOp["ops"][0]["q"])); // verify query is empty in oplog

    // fast update with additional query that matches
    assert.eq(0, wdb.foo.count({_id : 5})); // sanity check
    wdb.foo.insert({_id : 5, a : 5, b : "something"});
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({_id : 5, a : 5}, { $set : {b : "other"}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    x = wdb.foo.find({_id : 5}).next();
    assert.eq(x.b, "other"); // make sure the doc is properly changed
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    assert(friendlyEqual({_id : 5, a : 5}, lastOp["ops"][0]["q"])); // verify query is in oplog

    // these set of tests verify that updates that shouldn't be fast are not fast
    // this is handles in updateRepl.js

    // simple upsert test
    assert.eq(0, wdb.foo.count({_id : 6})); // sanity check
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({_id : 6}, { $inc : {a : 1}}, {upsert : 1});
    assertUpsertFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    assert.eq(1, wdb.foo.count({_id : 6})); // sanity check that doc exists
    val = wdb.foo.find({_id : 6}).next();
    assert(val.a == 1);
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    // check that an upsert that is an update works
    wdb.foo.update({_id : 6}, { $inc : {a : 1}}, {upsert : 1});
    assertUpsertFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    assert.eq(1, wdb.foo.count({_id : 6})); // sanity check that doc still doesn't exist
    val = wdb.foo.find({_id : 6}).next();
    assert(val.a == 2);

    wdb.foo.ensureIndex({_id : "hashed"}); // add a hashed id index, and make sure that upsert still works

    assert.eq(0, wdb.foo.count({_id : 7})); // sanity check
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({_id : 7}, { $inc : {a : 1}}, {upsert : 1});
    assertUpsertFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    assert.eq(1, wdb.foo.count({_id : 7})); // sanity check that doc exists
    val = wdb.foo.find({_id : 7}).next();
    assert(val.a == 1);
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    // check that an upsert that is an update works
    wdb.foo.update({_id : 7}, { $inc : {a : 1}}, {upsert : 1});
    assertUpsertFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    assert.eq(1, wdb.foo.count({_id : 7})); // sanity check that doc still doesn't exist
    val = wdb.foo.find({_id : 7}).next();
    assert(val.a == 2);


    print("fastUpdateRepl.js SUCCESS");
    replTest.stopSet(signal);
};

doCustomPKTest = function (signal, txnLimit, startPort, secondaryHasIndex) {
    var num = 3;
    var host = getHostName();
    var name = "rollback_unit";
    var timeout = 60000;

    var replTest = new ReplSetTest( {name: name, nodes: num, startPort:startPort, txnMemLimit: txnLimit} );
    var conns = replTest.startSet();
    var port = replTest.ports;
    var config = {_id : name, members :
            [
             {_id:0, host : host+":"+port[0], priority:10 },
             {_id:1, host : host+":"+port[1]},
             {_id:2, host : host+":"+port[2], arbiterOnly : true},
            ],
             };

    replTest.initiate(config);
    replTest.awaitReplication();
    assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });

    // Make sure we have a master
    conns[0].setSlaveOk();
    conns[1].setSlaveOk();

    // Make sure we have an arbiter
    assert.soon(function () {
        res = conns[2].getDB("admin").runCommand({ replSetGetStatus: 1 });
        return res.myState == 7;
    }, "Arbiter failed to initialize.");

    var wdb = conns[0].getDB("foo");
    assert.commandWorked(wdb.createCollection("foo", {primaryKey : {a : 1, _id : 1}}));
    replTest.awaitReplication();
    if (secondaryHasIndex) {
        // make all the updates we do be non-indexed on primary
        // but indexed on secondary
        print("shutting down secondary");
        replTest.stop(1);
        print("restarting secondary without replset");
        replTest.restartWithoutReplset(1);
        conns[1].getDB("foo").foo.ensureIndex({b:1});
        assert(!conns[1].getDB("foo").getLastError());
        // bring it back up
        print("shutting down secondary");
        replTest.stop(1);
        print("restarting secondary in replset");
        replTest.restart(1);
        assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });
    }

    // Make sure we have a master
    conns[0].setSlaveOk();
    conns[1].setSlaveOk();

    var swdb = conns[1].getDB("foo");
    var localdb = conns[0].getDB("local");
    // first basic test, that fast updates work only when enabled
    wdb.foo.insert({_id : 0, a : 0, b : 0});
    //update of non-existent key should not work
    wdb.foo.update({_id : 1}, { $set : {a : 1}});
    assertUpdateSlow(wdb);
    // now let's do an update that works, verify that it is slow
    wdb.foo.update({_id : 0, a : 0}, { $set : {b : 1}});
    assertOplogEntrySlow(localdb);

    // now let's turn on fast updates, do all the above things, and verify that they are indeed fast
    // this is a sanity check to verify that the setParameter works
    assert.commandWorked(wdb.adminCommand({ setParameter: 1, fastUpdates: true }));
    wdb.foo.update({_id : 0, a : 0}, { $set : {b : 100}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");

    wdb.foo.update({_id : 1, a : 1}, { $set : {b : 1}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");

    // now we've tested that the fastUpdates parameter works and that an update is fast only
    // if the flag is set. Now let's get to testing fast updates

    // these set of tests verify that operations that result in no changes are still fast and perform
    // message injections and oplog writes, and we verify that oplog entries are right

    // update with a query filter that does not match, even though the doc exists (_id : x, nonExistentField : 1)
    wdb.foo.insert({_id : 2, a : 2, b : 20});
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({_id : 2, a : 2, nonExistentField : 1}, { $set : {b : "abra"}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    x = wdb.foo.find({_id : 2}).next();
    assert.eq(x.b, 20); // make sure the doc is unchanged
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    assert(friendlyEqual({_id : 2, a : 2, nonExistentField : 1}, lastOp["ops"][0]["q"])); // verify full query made it to oplog

    // update that does something illegal, like $inc a text field
    assert.eq(0, wdb.foo.count({_id : 3})); // sanity check
    wdb.foo.insert({_id : 3, a : 3, b : "asdf"});
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({_id : 3, a : 3}, { $inc : {b : 1}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    x = wdb.foo.find({_id : 3, a : 3}).next();
    assert.eq(x.b, "asdf"); // make sure the doc is unchanged
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    // one day, we may optimize tokumx to not put the query in this oplog entry (or in the update message)
    assert(friendlyEqual({_id : 3, a : 3}, lastOp["ops"][0]["q"])); // verify query made it to oplog,

    // update on a non-existing document
    assert.eq(0, wdb.foo.count({a : 4 , _id : 4})); // sanity check
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({a : 4 , _id : 4}, { $inc : {b : 1}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    assert.eq(0, wdb.foo.count({a : 4 , _id : 4})); // sanity check that doc still doesn't exist
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    printjson(lastOp["ops"][0]["q"]);
    assert(friendlyEqual({_id : 4, a : 4}, lastOp["ops"][0]["q"])); // verify no query made it to oplog

    // verify that an update using just the _id is slow, and not fast
    assert.eq(1, wdb.foo.count({_id : 3})); // sanity check
    wdb.foo.update({_id : 3}, {$set : { b : "fdsa"}});
    assertUpdateFast(wdb, localdb); // the update should still be fast
    lastOp = getLastOp(localdb);
    printjson(lastOp);
    assert.eq(1, lastOp["ops"][0]["f"]); // flag should indicate that it was not done by PK

    // these set of tests shows that fast updates work on non-expecting scenarios:

    // normal fast update with no additional query, query is empty in oplog
    assert.eq(0, wdb.foo.count({_id : 4})); // sanity check
    wdb.foo.insert({_id : 4, a : 4, b : 40});
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({_id : 4 , a : 4}, { $inc : {b : 1}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    x = wdb.foo.find({a : 4, _id : 4}).next();
    assert.eq(x.b, 41); // make sure the doc is properly changed
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    assert(friendlyEqual({_id : 4, a : 4}, lastOp["ops"][0]["q"])); // verify query is empty in oplog

    // fast update with additional query that matches
    assert.eq(0, wdb.foo.count({_id : 5})); // sanity check
    wdb.foo.insert({_id : 5, a : 5, aa : 50, b : "something"});
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({_id : 5, a : 5, aa : 50}, { $set : {b : "other"}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    x = wdb.foo.find({a : 5, _id : 5}).next();
    assert.eq(x.b, "other"); // make sure the doc is properly changed
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    assert(friendlyEqual({_id : 5, a : 5, aa : 50}, lastOp["ops"][0]["q"])); // verify query is in oplog

    // these set of tests verify that updates that shouldn't be fast are not fast
    // this is handles in updateRepl.js    

    print("fastUpdateRepl.js SUCCESS");
    replTest.stopSet(signal);
};

doSecondaryKeyTest = function (signal, txnLimit, startPort, secondaryHasIndex) {
    var num = 3;
    var host = getHostName();
    var name = "rollback_unit";
    var timeout = 60000;

    var replTest = new ReplSetTest( {name: name, nodes: num, startPort:startPort, txnMemLimit: txnLimit} );
    var conns = replTest.startSet();
    var port = replTest.ports;
    var config = {_id : name, members :
            [
             {_id:0, host : host+":"+port[0], priority:10 },
             {_id:1, host : host+":"+port[1]},
             {_id:2, host : host+":"+port[2], arbiterOnly : true},
            ],
             };

    replTest.initiate(config);
    replTest.awaitReplication();
    assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });


    // Make sure we have an arbiter
    assert.soon(function () {
        res = conns[2].getDB("admin").runCommand({ replSetGetStatus: 1 });
        return res.myState == 7;
    }, "Arbiter failed to initialize.");

    var wdb = conns[0].getDB("foo");
    assert.commandWorked(wdb.createCollection("foo"));
    wdb.foo.ensureIndex({a : 1});
    replTest.awaitReplication();
    if (secondaryHasIndex) {
        // make all the updates we do be non-indexed on primary
        // but indexed on secondary
        print("shutting down secondary");
        replTest.stop(1);
        print("restarting secondary without replset");
        replTest.restartWithoutReplset(1);
        conns[1].getDB("foo").foo.ensureIndex({b:1});
        assert(!conns[1].getDB("foo").getLastError());
        // bring it back up
        print("shutting down secondary");
        replTest.stop(1);
        print("restarting secondary in replset");
        replTest.restart(1);
        assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });
    }
    conns[0].setSlaveOk();
    conns[1].setSlaveOk();

    var swdb = conns[1].getDB("foo");
    var localdb = conns[0].getDB("local");
    // first basic test, that fast updates work only when enabled

    wdb.foo.insert({_id : 0, a : 0, b : 1});
    //update of non-existent key should not work
    wdb.foo.update({a : 1}, { $set : {b : 100}});
    assertUpdateSlow(wdb);
    // now let's do an update that works, verify that it is slow
    wdb.foo.update({a : 0}, { $set : {b : 100}});
    assertOplogEntrySlow(localdb);

    // now let's turn on fast updates, do all the above things, and verify that they are indeed fast
    // this is a sanity check to verify that the setParameter works
    assert.commandWorked(wdb.adminCommand({ setParameter: 1, fastUpdates: true }));
    wdb.foo.update({a : 1}, { $set : {b : 100}});
    assertUpdateSlow(wdb, localdb); // this should result in no update, because we couldn't find a pk that matches
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");

    wdb.foo.update({a : 0}, { $set : {b : 10000}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");

    // now we've tested that the fastUpdates parameter works and that an update is fast only
    // if the flag is set. Now let's get to testing fast updates

    // these set of tests verify that operations that result in no changes are still fast and perform
    // message injections and oplog writes, and we verify that oplog entries are right

    // update with a query filter that does not match, even though the doc exists (_id : x, nonExistentField : 1)
    wdb.foo.insert({_id : 2, a : 2, b : 2});
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({a : 2, nonExistentField : 1}, { $set : {b : "abra"}});
    assertUpdateSlow(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    x = wdb.foo.find({_id : 2}).next();
    assert.eq(x.b, 2); // make sure the doc is unchanged
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount); // assert nothing was logged

    // update that does something illegal, like $inc a text field
    assert.eq(0, wdb.foo.count({_id : 3})); // sanity check
    wdb.foo.insert({_id : 3, a : 3, b : "asdf"});
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({a : 3}, { $inc : {b : 1}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    x = wdb.foo.find({_id : 3}).next();
    assert.eq(x.b, "asdf"); // make sure the doc is unchanged
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    assert(friendlyEqual({}, lastOp["ops"][0]["q"])); // verify no query made it to oplog

    // update on a non-existing document
    assert.eq(0, wdb.foo.count({_id : 4})); // sanity check
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({a : 4}, { $inc : {b : 1}});
    assertUpdateSlow(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    assert.eq(0, wdb.foo.count({_id : 4})); // sanity check that doc still doesn't exist
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount); // assert nothing was logged


    // these set of tests shows that fast updates work on non-expecting scenarios:

    // normal fast update with no additional query, query is empty in oplog
    assert.eq(0, wdb.foo.count({a : 4})); // sanity check
    wdb.foo.insert({_id : 4, a : 4, b : 2});
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({a : 4}, { $inc : {b : 1}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    x = wdb.foo.find({_id : 4}).next();
    assert.eq(x.b, 3); // make sure the doc is properly changed
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    assert(friendlyEqual({}, lastOp["ops"][0]["q"])); // verify query is empty in oplog
    assert.eq(1, lastOp["ops"][0]["f"]); // correct flag

    // fast update with additional query that matches
    assert.eq(0, wdb.foo.count({_id : 5})); // sanity check
    wdb.foo.insert({_id : 5, a : 5, aa: 55, b : "something"});
    wdb.getLastError();
    oldOplogCount = localdb.oplog.rs.count();
    wdb.foo.update({a : 5, aa : 55}, { $set : {b : "other"}});
    assertUpdateFast(wdb, localdb);
    replTest.awaitReplication();
    assert( dbs_match(wdb,swdb), "server data sets do not match after rollback, something is wrong");
    x = wdb.foo.find({_id : 5}).next();
    assert.eq(x.b, "other"); // make sure the doc is properly changed
    newOplogCount = localdb.oplog.rs.count();
    assert.eq(newOplogCount, oldOplogCount+1); // assert something was logged
    lastOp = getLastOp(localdb);
    assert(friendlyEqual({}, lastOp["ops"][0]["q"])); // no query is in oplog
    assert.eq(1, lastOp["ops"][0]["f"]); // correct flag

    // these set of tests verify that updates that shouldn't be fast are not fast
    // this is handles in updateRepl.js    

    print("fastUpdateRepl.js SUCCESS");
    replTest.stopSet(signal);
};

//doCustomPKTest(15, 1000000, 31000, true);
//doCustomPKTest(15, 1000000, 31000, false);
doIDPKTest(15, 1000000, 31000, true);
doIDPKTest(15, 1000000, 31000, false);
//doSecondaryKeyTest(15, 1000000, 31000, true);
//doSecondaryKeyTest(15, 1000000, 31000, false);
