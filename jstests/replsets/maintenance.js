

var replTest = new ReplSetTest( {name: 'unicomplex', nodes: 2} );
var conns = replTest.startSet();
replTest.initiate();

// Make sure we have a master
var master = replTest.getMaster();

for (i=0;i<10000; i++) { master.getDB("bar").foo.insert({x:1,y:i,abc:123,str:"foo bar baz"}); }
for (i=0;i<1000; i++) { master.getDB("bar").foo.update({y:i},{$push :{foo : "barrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr"}}); }
master.getDB("bar").createCollection("abra", {partitioned:1});
master.getDB("bar").abra.insert([{},{},{}]);
master.getDB("bar").abra.ensureIndex({a:1});
assert.commandWorked(master.getDB("bar").abra.addPartition());
master.getDB("bar").abra.insert([{},{},{}]);

replTest.awaitReplication();

assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });

print("use replSetMaintenance command to go in/out of maintence mode");

print("primary cannot go into maintence mode");
result = master.getDB("admin").runCommand({replSetMaintenance : 1});
assert.eq(result.ok, 0, tojson(result));

print("check getMore works on a secondary, not on a recovering node");
var cursor = conns[1].getDB("bar").foo.find();
for (var i=0; i<50; i++) {
    cursor.next();
}

print("secondary can");
result = conns[1].getDB("admin").runCommand({replSetMaintenance : 1});
assert.eq(result.ok, 1, tojson(result));

print("make sure secondary goes into recovering");
var x = 0;
assert.soon(function() {
    var im = conns[1].getDB("admin").isMaster();
    if (x++ % 5 == 0) printjson(im);
    return !im.secondary && !im.ismaster;
});

print("now getmore shouldn't work");
lastDoc = null;
while (cursor.hasNext()) {
    lastDoc = cursor.next();
}

// now let's verify that some fileops commands don't work
print("verify some commands fail");
assert.commandFailed(conns[1].getDB("bar").abra.addPartition());
assert.throws(conns[1].getDB("bar").abra.dropPartition(0));
assert.throws(conns[1].getDB("bar").dropDatabase());

print("the shell is currently stupid and won't throw once it's returned any query results");
printjson(lastDoc);
assert("$err" in lastDoc);
assert.eq(lastDoc.code, 13436);

result = conns[1].getDB("admin").runCommand({replSetMaintenance : 0});
assert.eq(result.ok, 1, tojson(result));

var secondarySoon = function() {
    var x = 0;
    assert.soon(function() {
        var im = conns[1].getDB("admin").isMaster();
                 if (x++ % 5 == 0) printjson(im);
                 return im.secondary;
             });
}; 

secondarySoon();

