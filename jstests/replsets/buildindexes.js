doTest = function( signal ) {

  var name = "buildIndexes";
  var host = getHostName();
  
  var replTest = new ReplSetTest( {name: name, nodes: 3} );

  var nodes = replTest.startSet();

  var config = replTest.getReplSetConfig();
  config.members[2].priority = 0;
  config.members[2].buildIndexes = false;
  
  replTest.initiate(config);

  var master = replTest.getMaster().getDB(name);
  var slaveConns = replTest.liveNodes.slaves;
  var slave = [];
  for (var i in slaveConns) {
    slaveConns[i].setSlaveOk();
    slave.push(slaveConns[i].getDB(name));
  }
  replTest.awaitReplication();

  print("creating an index on x");
  master.x.ensureIndex({y : 1});
  printjson(master.x.stats());

  for (var i=0; i<100; i++) {
    master.x.insert({x:1,y:"abc",c:1});
  }

  replTest.awaitReplication();

  printjson(slave[0].runCommand({count: "x"}));
  var ns = master.x+"";
  print("namespace: "+ns);

  // can't query system.indexes from slave, so we'll look at coll.stats()
  printjson(slave[0].adminCommand({replSetGetStatus:1}));
  printjson(slave[0].getSisterDB("local").system.replset.findOne());
  printjson(master.stats());
  printjson(slave[0].stats());
  printjson(slave[1].stats());
  printjson(master.x.stats());
  printjson(slave[0].x.stats());
  printjson(slave[1].x.stats());
  print("sleeping");  
  sleep(20000);
  var indexes = slave[0].stats().indexes;
  assert.eq(indexes, 4, 'number of indexes');

  indexes = slave[1].stats().indexes;
  assert.eq(indexes, 3);

  assert.commandWorked(master.createCollection("foo", {primaryKey : {a : 1, _id : 1}}));
  replTest.awaitReplication();
  assert(friendlyEqual(slave[1].foo.getIndexes(), master.foo.getIndexes()));

  replTest.stopSet(15);
}

doTest(15);
