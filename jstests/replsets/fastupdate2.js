// Test that a fastupdate may run on the master, but it gets
// converted to a regular update on the secondary because
// there are different indexes.

var name = "pk";
var host = getHostName();

var replTest = new ReplSetTest( {name: name, nodes: 2} );

var nodes = replTest.startSet();

var config = replTest.getReplSetConfig();
config.members[1].priority = 0;
config.members[1].buildIndexes = true;

replTest.initiate(config);

var primary = replTest.getMaster().getDB(name);
var slaveConns = replTest.liveNodes.slaves;
slaveConns[0].setSlaveOk();
var secondary = slaveConns[0].getDB(name);

primary.x.drop();
primary.x.ensureIndex({ z: 1 });
primary.x.insert({ _id: 0, c: 0, z: 500 });
replTest.awaitReplication();

// Pull the secondary out of the replset and add an index on { c : 1 }
restartSlaveOutOfReplset = function (replTest) {
    // stop secondary, bring it up outside of repl set
    // and add index
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary without replset");
    replTest.restartWithoutReplset(1);
}

restartSlaveInReplset = function(replTest, conns) {
    print("shutting down secondary");
    replTest.stop(1);
    print("restarting secondary in replset");
    replTest.restart(1);
    assert.soon(function() { return conns[1].getDB("admin").isMaster().secondary; });
}

restartSlaveOutOfReplset(replTest);
nodes[1].getDB(name).x.ensureIndex({ c: 1 });
assert.eq(null, nodes[1].getDB(name).getLastError());
restartSlaveInReplset(replTest, nodes);

// reset variables since we restart the replset above
primary = replTest.getMaster().getDB(name);
slaveConns = replTest.liveNodes.slaves;
slaveConns[0].setSlaveOk();
secondary = replTest.liveNodes.slaves[0].getDB(name);

// primary can fastupdate c since it is unindexed
assert.commandWorked(primary.adminCommand({ setParameter: 1, fastUpdates: true }));
primary.x.update({ _id: 0 }, { $inc: { c: 1 } });
replTest.awaitReplication();
assert.commandWorked(primary.adminCommand({ setParameter: 1, fastUpdates: false }));

// primary should be able to find { c: 1 } or { z: 500 } using _id or z index
assert.eq(1, primary.x.find({ c: 1 }).hint({ _id: 1 }).itcount());
assert.eq(1, primary.x.find({ c: 1 }).hint({ z: 1 }).itcount());
assert.eq(1, primary.x.find({ z: 500 }).hint({ _id: 1 }).itcount());
assert.eq(1, primary.x.find({ z: 500 }).hint({ z: 1 }).itcount());
// secondary should be able to find { c: 1 } or { z: 500 } using _id or z or c index
assert.eq(1, secondary.x.find({ c: 1 }).hint({ _id: 1 }).itcount());
assert.eq(1, secondary.x.find({ c: 1 }).hint({ z: 1 }).itcount());
assert.eq(1, secondary.x.find({ c: 1 }).hint({ c: 1 }).itcount());
assert.eq(1, secondary.x.find({ z: 500 }).hint({ _id: 1 }).itcount());
assert.eq(1, secondary.x.find({ z: 500 }).hint({ z: 1 }).itcount());
assert.eq(1, secondary.x.find({ z: 500 }).hint({ c: 1 }).itcount());

replTest.stopSet(15);
