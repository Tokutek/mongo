//
// Tests mongos's failure tolerance for replica set shards and read preference queries
//
// Sets up a cluster with three shards, the first shard of which has an unsharded collection and
// half a sharded collection.  The second shard has the second half of the sharded collection, and
// the third shard has nothing.  Progressively shuts down the primary of each shard to see the
// impact on the cluster.
//
// Three different connection states are tested - active (connection is active through whole
// sequence), idle (connection is connected but not used before a shard change), and new
// (connection connected after shard change).
//

var options = {separateConfig : true,
               rs : true,
               rsOptions : { nodes : 2 }};

var st = new ShardingTest({shards : 3, mongos : 1, other : options});
st.stopBalancer();

var mongos = st.s0;
var admin = mongos.getDB( "admin" );
var shards = mongos.getDB( "config" ).shards.find().toArray();

assert.commandWorked( admin.runCommand({ setParameter : 1, traceExceptions : true }) );
assert.commandWorked( admin.runCommand({ setParameter : 1, ignoreInitialVersionFailure : true }) );
assert.commandWorked( admin.runCommand({ setParameter : 1, authOnPrimaryOnly : false }) );

var collSharded = mongos.getCollection( "fooSharded.barSharded" );
var collUnsharded = mongos.getCollection( "fooUnsharded.barUnsharded" );

// Create the unsharded database
collUnsharded.insert({ some : "doc" });
assert.eq( null, collUnsharded.getDB().getLastError() );
collUnsharded.remove({});
assert.eq( null, collUnsharded.getDB().getLastError() );
printjson( admin.runCommand({ movePrimary : collUnsharded.getDB().toString(),
                              to : shards[0]._id }) );

// Create the sharded database
assert.commandWorked( admin.runCommand({ enableSharding : collSharded.getDB().toString() }) );
printjson( admin.runCommand({ movePrimary : collSharded.getDB().toString(), to : shards[0]._id }) );
assert.commandWorked( admin.runCommand({ shardCollection : collSharded.toString(),
                                         key : { _id : 1 } }) );
assert.commandWorked( admin.runCommand({ split : collSharded.toString(), middle : { _id : 0 } }) );
assert.commandWorked( admin.runCommand({ moveChunk : collSharded.toString(),
                                         find : { _id : 0 },
                                         to : shards[1]._id }) );

st.printShardingStatus();

// Needed b/c the GLE command itself can fail if the shard is down ("write result unknown") - we
// don't care if this happens in this test, we only care that we did not get "write succeeded".
// Depending on the connection pool state, we could get either.
function gleErrorOrThrow(database, msg) {
    var gle;
    try {
        gle = database.getLastErrorObj();
    }
    catch (ex) {
        return;
    }
    if (!gle.err) doassert("getLastError is null: " + tojson(gle) + " :" + msg);
    return;
};

//
// Setup is complete
//

jsTest.log("Inserting initial data...");

var mongosConnActive = new Mongo( mongos.host );
var mongosConnIdle = null;
var mongosConnNew = null;

mongosConnActive.getCollection( collSharded.toString() ).insert({ _id : -1 });
mongosConnActive.getCollection( collSharded.toString() ).insert({ _id : 1 });
assert.eq(null, mongosConnActive.getCollection( collSharded.toString() ).getDB().getLastError());

mongosConnActive.getCollection( collUnsharded.toString() ).insert({ _id : 1 });
assert.eq(null, mongosConnActive.getCollection( collUnsharded.toString() ).getDB().getLastError());

jsTest.log("Stopping primary of third shard...");

mongosConnIdle = new Mongo( mongos.host );

st.rs2.stop(st.rs2.getPrimary(), true /*wait for stop*/ );

jsTest.log("Testing active connection with third primary down...");

assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
assert.neq(null, mongosConnActive.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

mongosConnActive.getCollection( collSharded.toString() ).insert({ _id : -2 });
assert.gleSuccess(mongosConnActive.getCollection( collSharded.toString() ).getDB());
mongosConnActive.getCollection( collSharded.toString() ).insert({ _id : 2 });
assert.gleSuccess(mongosConnActive.getCollection( collSharded.toString() ).getDB());
mongosConnActive.getCollection( collUnsharded.toString() ).insert({ _id : 2 });
assert.gleSuccess(mongosConnActive.getCollection( collUnsharded.toString() ).getDB());

jsTest.log("Testing idle connection with third primary down...");

mongosConnIdle.getCollection( collSharded.toString() ).insert({ _id : -3 });
assert.gleSuccess(mongosConnIdle.getCollection( collSharded.toString() ).getDB());
mongosConnIdle.getCollection( collSharded.toString() ).insert({ _id : 3 });
assert.gleSuccess(mongosConnIdle.getCollection( collSharded.toString() ).getDB());
mongosConnIdle.getCollection( collUnsharded.toString() ).insert({ _id : 3 });
assert.gleSuccess(mongosConnIdle.getCollection( collUnsharded.toString() ).getDB());

assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : -1 }) );
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : 1 }) );
assert.neq(null, mongosConnIdle.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }) );

jsTest.log("Testing new connections with third primary down...");

mongosConnNew = new Mongo( mongos.host );
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : -1 }) );
mongosConnNew = new Mongo( mongos.host );
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : 1 }) );
mongosConnNew = new Mongo( mongos.host );
assert.neq(null, mongosConnNew.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }) );

mongosConnNew = new Mongo( mongos.host );
mongosConnNew.getCollection( collSharded.toString() ).insert({ _id : -4 });
assert.gleSuccess(mongosConnNew.getCollection( collSharded.toString() ).getDB());
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.getCollection( collSharded.toString() ).insert({ _id : 4 });
assert.gleSuccess(mongosConnNew.getCollection( collSharded.toString() ).getDB());
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.getCollection( collUnsharded.toString() ).insert({ _id : 4 });
assert.gleSuccess(mongosConnNew.getCollection( collUnsharded.toString() ).getDB());

gc(); // Clean up new connections

jsTest.log("Stopping primary of second shard...");

mongosConnIdle = new Mongo( mongos.host );

// Need to save this node for later
var rs1Secondary = st.rs1.getSecondary();

st.rs1.stop(st.rs1.getPrimary(), true /* wait for stop */);

jsTest.log("Testing active connection with second primary down...");

// Reads with read prefs
mongosConnActive.setSlaveOk();
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
assert.neq(null, mongosConnActive.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));
mongosConnActive.setSlaveOk(false);

mongosConnActive.setReadPref("primary");
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.throws(function() {
    mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : 1 });
});
assert.neq(null, mongosConnActive.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

//Ensure read prefs override slaveOK
mongosConnActive.setSlaveOk();
mongosConnActive.setReadPref("primary");
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.throws(function() {
    mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : 1 });
});
assert.neq(null, mongosConnActive.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));
mongosConnActive.setSlaveOk(false);

mongosConnActive.setReadPref("secondary");
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
assert.neq(null, mongosConnActive.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

mongosConnActive.setReadPref("primaryPreferred");
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
assert.neq(null, mongosConnActive.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

mongosConnActive.setReadPref("secondaryPreferred");
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
assert.neq(null, mongosConnActive.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

mongosConnActive.setReadPref("nearest");
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
assert.neq(null, mongosConnActive.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

// Writes
mongosConnActive.getCollection( collSharded.toString() ).insert({ _id : -5 });
assert.gleSuccess(mongosConnActive.getCollection( collSharded.toString() ).getDB());
mongosConnActive.getCollection( collSharded.toString() ).insert({ _id : 5 });
gleErrorOrThrow(mongosConnActive.getCollection( collSharded.toString() ).getDB());
mongosConnActive.getCollection( collUnsharded.toString() ).insert({ _id : 5 });
assert.gleSuccess(mongosConnActive.getCollection( collUnsharded.toString() ).getDB());

jsTest.log("Testing idle connection with second primary down...");

// Writes
mongosConnIdle.getCollection( collSharded.toString() ).insert({ _id : -6 });
assert.gleSuccess(mongosConnIdle.getCollection( collSharded.toString() ).getDB());
mongosConnIdle.getCollection( collSharded.toString() ).insert({ _id : 6 });
gleErrorOrThrow(mongosConnIdle.getCollection( collSharded.toString() ).getDB());
mongosConnIdle.getCollection( collUnsharded.toString() ).insert({ _id : 6 });
assert.gleSuccess(mongosConnIdle.getCollection( collUnsharded.toString() ).getDB());

// Reads with read prefs
mongosConnIdle.setSlaveOk();
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : -1 }) );
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : 1 }) );
assert.neq(null, mongosConnIdle.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }) );
mongosConnIdle.setSlaveOk(false);

mongosConnIdle.setReadPref("primary");
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.throws(function() {
    mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : 1 });
});
assert.neq(null, mongosConnIdle.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

// Ensure read prefs override slaveOK
mongosConnIdle.setSlaveOk();
mongosConnIdle.setReadPref("primary");
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.throws(function() {
    mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : 1 });
});
assert.neq(null, mongosConnIdle.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));
mongosConnIdle.setSlaveOk(false);

mongosConnIdle.setReadPref("secondary");
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
assert.neq(null, mongosConnIdle.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

mongosConnIdle.setReadPref("primaryPreferred");
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
assert.neq(null, mongosConnIdle.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

mongosConnIdle.setReadPref("secondaryPreferred");
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
assert.neq(null, mongosConnIdle.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

mongosConnIdle.setReadPref("nearest");
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
assert.neq(null, mongosConnIdle.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

jsTest.log("Testing new connections with second primary down...");

// Reads with read prefs
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setSlaveOk();
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : -1 }) );
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setSlaveOk();
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : 1 }) );
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setSlaveOk();
assert.neq(null, mongosConnNew.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }) );

mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("primary");
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("primary");
assert.throws(function() {
    mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : 1 });
});
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("primary");
assert.neq(null, mongosConnNew.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

// Ensure read prefs override slaveok
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setSlaveOk();
mongosConnNew.setReadPref("primary");
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setSlaveOk();
mongosConnNew.setReadPref("primary");
assert.throws(function() {
    mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : 1 });
});
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setSlaveOk();
mongosConnNew.setReadPref("primary");
assert.neq(null, mongosConnNew.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("secondary");
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("secondary");
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("secondary");
assert.neq(null, mongosConnNew.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("primaryPreferred");
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("primaryPreferred");
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("primaryPreferred");
assert.neq(null, mongosConnNew.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("secondaryPreferred");
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("secondaryPreferred");
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("secondaryPreferred");
assert.neq(null, mongosConnNew.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("nearest");
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("nearest");
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setReadPref("nearest");
assert.neq(null, mongosConnNew.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

// Writes
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.getCollection( collSharded.toString() ).insert({ _id : -7 });
assert.gleSuccess(mongosConnNew.getCollection( collSharded.toString() ).getDB());
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.getCollection( collSharded.toString() ).insert({ _id : 7 });
gleErrorOrThrow(mongosConnNew.getCollection( collSharded.toString() ).getDB());
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.getCollection( collUnsharded.toString() ).insert({ _id : 7 });
assert.gleSuccess(mongosConnNew.getCollection( collUnsharded.toString() ).getDB());

gc(); // Clean up new connections

jsTest.log("Stopping primary of first shard...");

mongosConnIdle = new Mongo( mongos.host );

st.rs0.stop(st.rs0.getPrimary(), true /*wait for stop*/ );

jsTest.log("Testing active connection with first primary down...");

mongosConnActive.setSlaveOk();
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : 1 }));
assert.neq(null, mongosConnActive.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

mongosConnActive.getCollection( collSharded.toString() ).insert({ _id : -8 });
gleErrorOrThrow(mongosConnActive.getCollection( collSharded.toString() ).getDB());
mongosConnActive.getCollection( collSharded.toString() ).insert({ _id : 8 });
gleErrorOrThrow(mongosConnActive.getCollection( collSharded.toString() ).getDB());
mongosConnActive.getCollection( collUnsharded.toString() ).insert({ _id : 8 });
gleErrorOrThrow(mongosConnActive.getCollection( collUnsharded.toString() ).getDB());

jsTest.log("Testing idle connection with first primary down...");

mongosConnIdle.getCollection( collSharded.toString() ).insert({ _id : -9 });
gleErrorOrThrow(mongosConnIdle.getCollection( collSharded.toString() ).getDB());
mongosConnIdle.getCollection( collSharded.toString() ).insert({ _id : 9 });
gleErrorOrThrow(mongosConnIdle.getCollection( collSharded.toString() ).getDB());
mongosConnIdle.getCollection( collUnsharded.toString() ).insert({ _id : 9 });
gleErrorOrThrow(mongosConnIdle.getCollection( collUnsharded.toString() ).getDB());

mongosConnIdle.setSlaveOk();
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : -1 }) );
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : 1 }) );
assert.neq(null, mongosConnIdle.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }) );

jsTest.log("Testing new connections with first primary down...");

mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setSlaveOk();
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : -1 }) );
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setSlaveOk();
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : 1 }) );
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setSlaveOk();
assert.neq(null, mongosConnNew.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }) );

mongosConnNew = new Mongo( mongos.host );
mongosConnNew.getCollection( collSharded.toString() ).insert({ _id : -10 });
gleErrorOrThrow(mongosConnNew.getCollection( collSharded.toString() ).getDB());
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.getCollection( collSharded.toString() ).insert({ _id : 10 });
gleErrorOrThrow(mongosConnNew.getCollection( collSharded.toString() ).getDB());
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.getCollection( collUnsharded.toString() ).insert({ _id : 10 });
gleErrorOrThrow(mongosConnNew.getCollection( collUnsharded.toString() ).getDB());

gc(); // Clean up new connections

jsTest.log("Stopping second shard...");

mongosConnIdle = new Mongo( mongos.host );

st.rs1.stop(rs1Secondary, true /* wait for stop */);

jsTest.log("Testing active connection with second shard down...");

mongosConnActive.setSlaveOk();
assert.neq(null, mongosConnActive.getCollection( collSharded.toString() ).findOne({ _id : -1 }));
assert.neq(null, mongosConnActive.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }));

mongosConnActive.getCollection( collSharded.toString() ).insert({ _id : -11 });
gleErrorOrThrow(mongosConnActive.getCollection( collSharded.toString() ).getDB());
mongosConnActive.getCollection( collSharded.toString() ).insert({ _id : 11 });
gleErrorOrThrow(mongosConnActive.getCollection( collSharded.toString() ).getDB());
mongosConnActive.getCollection( collUnsharded.toString() ).insert({ _id : 11 });
gleErrorOrThrow(mongosConnActive.getCollection( collUnsharded.toString() ).getDB());

jsTest.log("Testing idle connection with second shard down...");

mongosConnIdle.getCollection( collSharded.toString() ).insert({ _id : -12 });
gleErrorOrThrow(mongosConnIdle.getCollection( collSharded.toString() ).getDB());
mongosConnIdle.getCollection( collSharded.toString() ).insert({ _id : 12 });
gleErrorOrThrow(mongosConnIdle.getCollection( collSharded.toString() ).getDB());
mongosConnIdle.getCollection( collUnsharded.toString() ).insert({ _id : 12 });
gleErrorOrThrow(mongosConnIdle.getCollection( collUnsharded.toString() ).getDB());

mongosConnIdle.setSlaveOk();
assert.neq(null, mongosConnIdle.getCollection( collSharded.toString() ).findOne({ _id : -1 }) );
assert.neq(null, mongosConnIdle.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }) );

jsTest.log("Testing new connections with second shard down...");

mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setSlaveOk();
assert.neq(null, mongosConnNew.getCollection( collSharded.toString() ).findOne({ _id : -1 }) );
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.setSlaveOk();
assert.neq(null, mongosConnNew.getCollection( collUnsharded.toString() ).findOne({ _id : 1 }) );

mongosConnNew = new Mongo( mongos.host );
mongosConnNew.getCollection( collSharded.toString() ).insert({ _id : -13 });
gleErrorOrThrow(mongosConnNew.getCollection( collSharded.toString() ).getDB());
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.getCollection( collSharded.toString() ).insert({ _id : 13 });
gleErrorOrThrow(mongosConnNew.getCollection( collSharded.toString() ).getDB());
mongosConnNew = new Mongo( mongos.host );
mongosConnNew.getCollection( collUnsharded.toString() ).insert({ _id : 13 });
gleErrorOrThrow(mongosConnNew.getCollection( collUnsharded.toString() ).getDB());

gc(); // Clean up new connections

jsTest.log("DONE!");
st.stop();
