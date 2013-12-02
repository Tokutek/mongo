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

primary.x.insert({ _id: 0, c: 1, z: 1 });
replTest.awaitReplication();
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
print('primary oplog contents: ');
primaryOplog.find().forEach(printjson);
print('secondary oplog contents: ');
secondaryOplog.find().forEach(printjson);

primaryOps = primaryOplog.findOne({ $and: [ { ops: { $elemMatch: { op: "i" } } }, { ops: { $elemMatch: { op: "d" } } } ] }).ops;
secondaryOps = secondaryOplog.findOne({ $and: [ { ops: { $elemMatch: { op: "i" } } }, { ops: { $elemMatch: { op: "d" } } } ] }).ops;
assert.eq({ _id: 0, c: 1, z: 1 }, primaryOps[0].o);
assert.eq({ _id: 0, c: 1, z: 0 }, primaryOps[1].o);
assert.eq({ _id: 0, c: 1, z: 1 }, secondaryOps[0].o);
assert.eq({ _id: 0, c: 1, z: 0 }, secondaryOps[1].o);

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
