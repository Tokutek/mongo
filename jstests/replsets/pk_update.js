var name = "pk";
var host = getHostName();

var replTest = new ReplSetTest( {name: name, nodes: 2} );

var nodes = replTest.startSet();

var config = replTest.getReplSetConfig();
config.members[1].priority = 0;
config.members[1].buildIndexes = true;

replTest.initiate(config);

var primary = replTest.getMaster().getDB(name);
var primaryOplog = replTest.getMaster().getDB("local").oplog.rs;
var slaveConns = replTest.liveNodes.slaves;
slaveConns[0].setSlaveOk();
var secondary = slaveConns[0].getDB(name);
var secondaryOplog = slaveConns[0].getDB("local").oplog.rs;

primary.x.drop();
assert.commandWorked(primary.x.runCommand({ create: 'x', primaryKey: { z: 1, _id: 1 }}));
primary.x.ensureIndex({ c: 1 });

primary.x.insert({ c: 1, z: 1 });
replTest.awaitReplication();
print("    after insert, primary oplog contains:");
primaryOplog.find().forEach(function(o) { printjson(o) } );
assert.eq(4, primaryOplog.find().count());
assert.eq(4, secondaryOplog.find().count());

assert.eq(1, primary.x.find({ z: 1 }).hint({ _id: 1 }).itcount());
assert.eq(1, primary.x.find({ z: 1 }).hint({ z: 1, _id: 1 }).itcount());
assert.eq(1, primary.x.find({ c: 1 }).hint({ c: 1 }).itcount());
assert.eq(1, secondary.x.find({ z: 1 }).hint({ _id: 1 }).itcount());
assert.eq(1, secondary.x.find({ z: 1 }).hint({ z: 1, _id: 1 }).itcount());
assert.eq(1, secondary.x.find({ c: 1 }).hint({ c: 1 }).itcount());

// update changes part of the pk, which results in logging a full delete
// and full insert to the oplog.
primary.x.update({ z: 1 }, { c: 1, z: 0 });
replTest.awaitReplication();

print("    after update, primary oplog contains:");
primaryOplog.find().forEach(function(o) { printjson(o) } );
assert.eq(5, primaryOplog.find().count());
assert.eq(5, secondaryOplog.find().count());
// Two operations should be included in the next oplog entry - first a delete, then an insert.
assert.eq("d", primaryOplog.find().skip(4)[0].ops[0].op);
assert.eq("i", primaryOplog.find().skip(4)[0].ops[1].op);
assert.eq("d", secondaryOplog.find().skip(4)[0].ops[0].op);
assert.eq("i", secondaryOplog.find().skip(4)[0].ops[1].op);

assert.eq(0, primary.x.find({ z: 1 }).hint({ _id: 1 }).itcount());
assert.eq(0, primary.x.find({ z: 1 }).hint({ z: 1, _id: 1 }).itcount());
assert.eq(1, primary.x.find({ c: 1 }).hint({ c: 1 }).itcount());
assert.eq(1, primary.x.find({ z: 0 }).hint({ _id: 1 }).itcount());
assert.eq(1, primary.x.find({ z: 0 }).hint({ z: 1, _id: 1 }).itcount());
assert.eq(0, secondary.x.find({ z: 1 }).hint({ _id: 1 }).itcount());
assert.eq(0, secondary.x.find({ z: 1 }).hint({ z: 1, _id: 1 }).itcount());
assert.eq(1, secondary.x.find({ c: 1 }).hint({ c: 1 }).itcount());
assert.eq(1, secondary.x.find({ z: 0 }).hint({ _id: 1 }).itcount());
assert.eq(1, secondary.x.find({ z: 0 }).hint({ z: 1, _id: 1 }).itcount());

replTest.stopSet(15);
