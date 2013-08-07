doTest = function( signal ) {

  var host = getHostName();
  var name="keepOplogAlive";
  var replTest = new ReplSetTest( {name: name, nodes: 1} );

  var nodes = replTest.startSet();

  var config = replTest.getReplSetConfig();
  
  replTest.initiate(config);

  var master = replTest.getMaster().getDB("local");
  var x = master.oplog.rs.find().sort({$natural:-1}).limit(1).next();
  master.runCommand({_testHooks:1, keepOplogAlive:1000});
  sleep(2000);
  var y = master.oplog.rs.find().sort({$natural:-1}).limit(1).next();
  assert(x._id != y._id);

  replTest.stopSet(15);
}

doTest(15);
