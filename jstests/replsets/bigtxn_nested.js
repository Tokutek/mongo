// verify that a txn that inserts 3 8MB documents can be replicated

doTest = function( signal ) {

  var name = "bigdoc";
  var host = getHostName();
  
  var replTest = new ReplSetTest( {name: name, nodes: 3} );

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

  print("insert small doc");
  a = '1'
  master.x.insert({k : a});

  print("insert big doc");
  b='b'; for (var i=0; i<23; i++) { b += b; }; // create an 8 MB string
  master.runCommand("beginTransaction");
  master.x.insert({k:b});
  master.x.insert({k:b});
  master.x.insert({k:b});
  master.runCommand("commitTransaction");

  replTest.awaitReplication();

  print("master");
  master.x.find().forEach(function (x) { print(x._id, x.k.length); } );
  master.x.find().forEach(function (x) { if (x.k.length == a.length) assert(x.k == a); else assert(x.k == b); } );
  for (var i in slaveConns) {
    print("slave", i);
    slave[i].x.find().forEach(function (x) { print(x._id, x.k.length); } );
    slave[i].x.find().forEach(function (x) { if (x.k.length == a.length) assert(x.k == a); else assert(x.k == b); } );
  }

  replTest.stopSet(15);
}

doTest(15);
