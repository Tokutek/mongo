// Test a large rollback SERVER-2737

var replTest = new ReplSetTest({ name: 'unicomplex', nodes: 3 });
var nodes = replTest.nodeList();

var conns = replTest.startSet();
var r = replTest.initiate({ "_id": "unicomplex",
                          "members": [
                                      { "_id": 0, "host": nodes[0], priority:10 },
                                      { "_id": 1, "host": nodes[1] },
                                      { "_id": 2, "host": nodes[2], arbiterOnly: true}]
                          });

// Make sure we have a master
var master = replTest.getMaster();
b_conn = conns[1];
b_conn.setSlaveOk();


var masterdb = master.getDB('db');
var slavedb = b_conn.getDB('db');

assert.commandWorked(masterdb.beginTransaction('mvcc'));
assert.commandWorked(masterdb.commitTransaction());
assert.commandWorked(masterdb.beginTransaction('serializable'));
assert.commandWorked(masterdb.commitTransaction());
assert.commandWorked(masterdb.beginTransaction('readUncommitted'));
assert.commandWorked(masterdb.commitTransaction());

assert.commandWorked(slavedb.beginTransaction('mvcc'));
assert.commandWorked(slavedb.commitTransaction());
assert.commandFailed(slavedb.beginTransaction('serializable'));
assert.commandWorked(slavedb.beginTransaction('readUncommitted'));
assert.commandWorked(slavedb.commitTransaction());


replTest.stopSet();

