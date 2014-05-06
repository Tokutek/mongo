// Test cloneCollection command

var baseName = "clonePartition";


ports = allocatePorts( 2 );

f = startMongod( "--port", ports[ 0 ], "--dbpath", "/data/db/" + baseName + "_from", "--nohttpinterface", "--bind_ip", "127.0.0.1" );
t = startMongod( "--port", ports[ 1 ], "--dbpath", "/data/db/" + baseName + "_to", "--nohttpinterface", "--bind_ip", "127.0.0.1" );

fooDB = f.getDB("foo");
adminDB = f.getDB("admin");

assert.commandWorked(fooDB.runCommand({create:"part", partitioned:1}));
assert.commandWorked(fooDB.runCommand({addPartition:"part", newMax:{_id: 10}}));
assert.commandWorked(fooDB.runCommand({addPartition:"part", newMax:{_id: 20}}));
assert.commandWorked(fooDB.runCommand({addPartition:"part", newMax:{_id: 30}}));
assert.commandWorked(fooDB.runCommand({addPartition:"part", newMax:{_id: 40}}));
assert.commandWorked(fooDB.runCommand({addPartition:"part", newMax:{_id: 50}}));
assert.commandWorked(fooDB.runCommand({dropPartition:"part", id:0}));

for( i = 0; i < 60; ++i ) {
    fooDB.part.insert( { _id: i } );
}
assert.eq(fooDB.part.count(), 60);

// first test copyDB
assert.commandWorked(adminDB.runCommand({copydb:1, fromhost: "localhost:" + ports[ 0 ], fromdb: "foo", todb: "bar"}));

print ("testing copydb");
barDB = f.getDB("bar");
// now let's check that both foo and bar have the same contents for collection part
assert.eq(fooDB.part.count(), 60);
assert.eq(barDB.part.count(), 60);

x = fooDB.runCommand({getPartitionInfo:"part"});
y = barDB.runCommand({getPartitionInfo:"part"});
assert(friendlyEqual(x,y));

print ("testing cloneCollection");
assert.commandWorked(t.getDB("foo").runCommand({cloneCollection:"foo.part", from:"localhost:" + ports[ 0 ]}));
assert.eq(t.getDB("foo").part.count(), 60);
y = t.getDB("foo").runCommand({getPartitionInfo:"part"});
assert(friendlyEqual(x,y));

print ("testing clone");

assert.commandWorked(t.getDB("bar").runCommand({clone:"localhost:" + ports[ 0 ]}));
y = t.getDB("bar").runCommand({getPartitionInfo:"part"});
assert.eq(t.getDB("bar").part.count(), 60);
assert(friendlyEqual(x,y));



