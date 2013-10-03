doTest = function( signal ) {

  var name = "bgindex";
  var host = getHostName();
  
  var replTest = new ReplSetTest( {name: name, nodes: 3} );

  var nodes = replTest.startSet();

  var config = replTest.getReplSetConfig();
  config.members[1].priority = 0;
  config.members[1].buildIndexes = true;
  config.members[2].priority = 0;
  config.members[2].buildIndexes = true;
  
  replTest.initiate(config);

  var master = replTest.getMaster().getDB(name);
  var slaveConns = replTest.liveNodes.slaves;
  var slave = [];
  for (var i in slaveConns) {
    slaveConns[i].setSlaveOk();
    slave.push(slaveConns[i].getDB(name));
  }
  replTest.awaitReplication();

  print("populating collection");
  for (var i=0; i<200000; i++) {
    if (i % 10000 == 0) print(" inserted " + i);
    master.x.insert({x:i, y: (Math.random() * 100)});
  }
  print("done populating. waiting for secondaries to catch up...");
  replTest.awaitReplication();

  print("creating a hot index on y");
  master.x.ensureIndex({y : 1}, {background: true});
  print("waiting for { w: 3 } so we know the ensure index is on each secondary");
  master.runCommand({ getLastError:1, w: 3 });
  print("{ w: 3 } satisfied. sleeping for 500 ms");
  sleep(500);
  print("trying to query secondaries...");
  st1 = slave[0].x.stats();
  st2 = slave[1].x.stats();
  assert(st1.nindexes == 1 && 
         st2.nindexes == 1 && 
         st1.nindexesbeingbuilt == 2 && // > nindexes, meaning build in progress
         st2.nindexesbeingbuilt == 2 && 
         slave[0].x.find().limit(1).itcount() == 1 &&
         slave[1].x.find().limit(1).itcount() == 1,
         'could not read from secondaries while their index builds were in progress');

  print("both secondaries responded to a query during their index builds");
  replTest.awaitReplication();
  print("both secondaries caught up in replication " +
        "this should take a few seconds after responding to queries, " +
        "or the test is not very good");

  replTest.stopSet(15);
}

doTest(15);
