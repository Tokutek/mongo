doTest = function( signal ) {

  var host = getHostName();
  var name="keepOplogAlive";
  var replTest = new ReplSetTest( {name: name, nodes: 1} );

  var nodes = replTest.startSet();

  var config = replTest.getReplSetConfig();
  
  replTest.initiate(config);

  var master = replTest.getMaster().getDB("admin");
  var x = master.runCommand({replGetExpireOplog:1});
  // These values represent the default of the system.
  // Should the defaults ever change, these values will
  // need to be updated
  assert.eq(x.expireOplogDays, 14);
  assert.eq(x.expireOplogHours, 0);

  master.runCommand({replSetExpireOplog:1, expireOplogDays:2, expireOplogHours:4});
  x = master.runCommand({replGetExpireOplog:1});
  assert(x.expireOplogDays == 2);
  assert(x.expireOplogHours == 4);

  replTest.stopSet(15);
}

doTest(15);
