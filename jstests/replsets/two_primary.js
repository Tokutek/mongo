// a test of rollback in replica sets
//
// try running as :
// 
//   mongo --nodb rollback2.js | tee out | grep -v ^m31
//

var debugging = 0;

function pause(s) {
    print(s);
    while (debugging) {
        sleep(3000);
        print(s);
    }
}

w = 0;

function wait(f) {
    w++;
    var n = 0;
    while (!f()) {
        if (n % 4 == 0)
            print("rollback2.js waiting " + w);
        if (++n == 4) {
            print("" + f);
        }
        assert(n < 200, 'tried 200 times, giving up');
        sleep(1000);
    }
}

/* these writes will be initial data and replicate everywhere. */
function doInitialWrites(db) {
    t = db.bar;
    t.insert({ q:0});
}

doTest = function (signal) {

    var num = 3;
    var host = getHostName();
    var name = "tags";
    var timeout = 60000;

    var replTest = new ReplSetTest( {name: name, nodes: num, startPort:31000} );
    var conns = replTest.startSet();
    var port = replTest.ports;
    var config = {_id : name, members :
            [
             {_id:0, host : host+":"+port[0]},
             {_id:1, host : host+":"+port[1]},
             {_id:2, host : host+":"+port[2], arbiterOnly : true},
            ],
             };

    replTest.initiate(config);

    replTest.awaitReplication();
    replTest.bridge();

    // Make sure we have a master
    var master = replTest.getMaster();
    var a_conn = conns[0];
    var b_conn = conns[1];
    var arb_conn = conns[2]
    a_conn.setSlaveOk();
    b_conn.setSlaveOk();
    var origMasterID;
    var newMasterID;
    if (master == a_conn) {
        origMasterID = 0;
        newMasterID = 1;
    }
    else {
        assert(b_conn == master);
        origMasterID = 1;
        newMasterID = 0;
    }


    // Wait for initial replication
    var a = conns[origMasterID].getDB("foo");
    var b = conns[newMasterID].getDB("foo");

    doInitialWrites(a);
    replTest.awaitReplication();

    print("disconnect primary from everywhere");
    replTest.partition(newMasterID, origMasterID);
    // make arbiter not be able to see primary, but allow primary
    // to see arbiter (C). This way, primary still sees majority of set
    // (itself and C), but because neither C nor B see A, C and B
    // should elect a new primary
    replTest.partition(2,origMasterID,false);
    // wait for b to become master
    print("wait for new master");
    assert.soon(function() { return conns[newMasterID].getDB("admin").isMaster().ismaster; });

    print("connect B back");
    replTest.unPartition(2,origMasterID,false);

    // let's verify that what was the original master is now secondary
    assert.soon(function() {var x = arb_conn.getDB("admin").runCommand({replSetGetStatus:1}); printjson(x); return x["members"][origMasterID]["state"] == 2});
    // A should be master because that is what was elected
    assert(conns[newMasterID].getDB("admin").isMaster().ismaster);

    pause("rollback_two_primaries.js SUCCESS");
    replTest.stopSet(signal);
    };

    print("rollback_majority_bug.js");

    doTest( 15 );
