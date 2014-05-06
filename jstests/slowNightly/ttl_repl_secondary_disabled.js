/** Test TTL docs are not deleted from secondaries directly
 */

var rt = new ReplSetTest({ name: 'ttl_repl_secondary_disabled', nodes: 3 });

// setup set
var connstrings = rt.nodeList();
var nodes = rt.startSet();
rt.initiate({_id: 'ttl_repl_secondary_disabled',
             members: [
                 { _id: 0, host: connstrings[0], priority: 10 },
                 { _id: 1, host: connstrings[1] },
                 { _id: 2, host: connstrings[2], arbiterOnly: true }
             ]});

var restartSlaveOutOfReplset = function(rst) {
    jsTest.log('restarting ' + connstrings[1] + ' out of replica set');
    rst.stop(1);
    rst.restartWithoutReplset(1);
};
var restartSlaveInReplset = function(rst) {
    jsTest.log('restarting ' + connstrings[1] + ' back in replica set');
    rst.stop(1);
    rst.restart(1);
};

var master = rt.getMaster();
rt.awaitSecondaryNodes();
var slave1 = nodes[1];

// shortcuts
var masterdb = master.getDB( 'd' );
var slave1db = slave1.getDB( 'd' );
var mastercol = masterdb[ 'c' ];
var slave1col = slave1db[ 'c' ];

restartSlaveOutOfReplset(rt);

// create TTL index, wait for TTL monitor to kick in, then check things
nodes[1].getDB('d')['c'].ensureIndex( { x : 1 } , { expireAfterSeconds : 10 } );

restartSlaveInReplset(rt);

var slaveadmin = nodes[1].getDB('admin');
assert.soon(function() { return slaveadmin.isMaster().secondary; });

//increase logging
slaveadmin.runCommand({setParameter:1, logLevel:1});

//insert old doc (10 minutes old) directly on secondary using godinsert
mastercol.insert({ _id: new Date(), x: new Date((new Date()).getTime() - 600000) });
rt.awaitReplication();
sleep(1000);

// need to grab these again after the restart
slave1db = nodes[1].getDB( 'd' );
slave1col = slave1db[ 'c' ];

assert.eq(1, slave1col.count(), "missing inserted doc" );

sleep(70*1000) //wait for 70seconds
assert.eq(1, slave1col.count(), "ttl deleted my doc!" );

// looking for this error : "Assertion: 13312:replSet error : logOp() but not primary"
// indicating that the secondary tried to delete the doc, but shouldn't be writing
var errorString = "13312";
var foundError = false;
var globalLogLines = slaveadmin.runCommand({getLog:"global"}).log
for (i in globalLogLines) {
    var line = globalLogLines[i];
    if (line.match( errorString )) {
        foundError = true;
        errorString = line; // replace error string with what we found.
        break;
    }
}

assert.eq(false, foundError, "found error in this line: " + errorString);

// finish up
rt.stopSet();
