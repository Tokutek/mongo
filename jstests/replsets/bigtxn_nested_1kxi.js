// verify that a big nested txn gets replicated

doTest = function( signal ) {

  var name = "bigtxn";
  var host = getHostName();
  
  var replTest = new ReplSetTest( {name: name, nodes: 3, txnMemLimit: 1000} );

  var nodes = replTest.startSet();

  var config = replTest.getReplSetConfig();
  
  replTest.initiate(config);

  var master = replTest.getMaster().getDB(name);
  var slaveConns = replTest.liveNodes.slaves;
  var slave = [];
  for (var i in slaveConns) {
    slaveConns[i].setSlaveOk();
    slave.push(slaveConns[i].getDB(name));
  }
  replTest.awaitReplication();

  print("insert into primary");
  var n = 1000;
  master.runCommand("beginTransaction");
  // do a bunch of inserts
  for (var i=1; i<=n; i++) {
    master.x.insert({i:i});
  }
  master.runCommand("commitTransaction");

  replTest.awaitReplication();

  // verify 
  assert.eq(n,master.x.count());
  var s = 0;
  master.x.find().forEach(function (x) { s+=x.i; });
  assert.eq(s, n*(n+1)/2);
  print("master", s);
  for (var i in slaveConns) {
    assert.eq(n,slave[i].x.count());
    var s = 0;
    slave[i].x.find().forEach(function (x) { s+=x.i; });
    assert.eq(s, n*(n+1)/2);
    print("slave", i, s);
  }

  replTest.stopSet(15);
}

doTest(15);
