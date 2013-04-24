assert(false, "this probably fails because the fsync command isn't implemented");

/* SERVER-4385
 * SyncClusterConnection should refresh sub-connections on recieving exceptions
 *
 * 1. Start 3 config servers.
 * 2. Create a syncclusterconnection to the servers from step 1.
 * 3. Restart one of the config servers.
 * 4. Try an insert. It should fail. This will also refresh the sub connection.
 * 5. Try an insert again. This should work fine.
 */

var mongoA = MongoRunner.runMongod({});
var mongoB = MongoRunner.runMongod({});
var mongoC = MongoRunner.runMongod({});
var mongoSCC = new Mongo(mongoA.host + "," + mongoB.host + "," + mongoC.host);

MongoRunner.stopMongod(mongoA);
MongoRunner.runMongod({ restart: mongoA.runId });

try {
    jsTest.log("this insert should fail");
    mongoSCC.getCollection("foo.bar").insert({ x : 1});
    jsTest.log("first insert didn't fail as it should have");
    assert(false , "must throw an insert exception");
} catch (e) {
    jsTest.log("first insert failed");
    printjson(e);
}

jsTest.log("this insert should succeed");
mongoSCC.getCollection("foo.bar").insert({ blah : "blah" });
jsTest.log("running GLE");
assert.eq(null, mongoSCC.getDB("foo").getLastError());
jsTest.log("second insert succeeded as it should have");
