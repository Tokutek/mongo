// a test of rollback in replica sets
//
// try running as :
// 
//   mongo --nodb rollback2.js | tee out | grep -v ^m31
//

var debugging = 0;

function ifReady(db, f) {
    var stats = db.adminCommand({ replSetGetStatus: 1 });
    

    // only eval if state isn't recovery
    if (stats && stats.myState != 3) {
        return f();
    }

    return false;
}

function pause(s) {
    print(s);
    while (debugging) {
        sleep(3000);
        print(s);
    }
}

function deb(obj) { 
    if( debugging ) {
        print("\n\n\n" + obj + "\n\n");
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

function dbs_match(a, b) {
    print("dbs_match");

    var ac = a.system.namespaces.find().sort({name:1}).toArray();
    var bc = b.system.namespaces.find().sort({name:1}).toArray();
	if (ac.length != bc.length) {
        print("dbs_match: namespaces don't match, lengths different");
        print("\n\n");
        printjson(ac);
        print("\n\n");
        printjson(bc);
        print("\n\n");
        return false;
    }
	for (var i = 0; i < ac.length; i++) {
		if (ac[i].name != bc[i].name) {
	        print("dbs_match: namespaces don't match");
	        print("\n\n");
	        printjson(ac);
	        print("\n\n");
	        printjson(bc);
	        print("\n\n");
	        return false;
		}
	}

    var c = a.getCollectionNames();
    for( var i in c ) {
        print("checking " + c[i]);
        if( !friendlyEqual( a[c[i]].find().sort({_id:1}).toArray(), b[c[i]].find().sort({_id:1}).toArray() ) ) { 
            print("dbs_match: collections don't match " + c[i]);
            return false;
        }
    }
    return true;
}

/* these writes will be initial data and replicate everywhere. */
function doInitialWrites(db) {
    t = db.bar;
    t.insert({ q:0});
    t.insert({ q: 1, a: "foo" });
    t.insert({ q: 2, a: "foo", x: 1 });
    t.insert({ q: 3, bb: 9, a: "foo" });
    t.insert({ q: 40, a: 1 });
    t.insert({ q: 40, a: 2 });
    t.insert({ q: 70, txt: 'willremove' });

    db.createCollection("kap", { capped: true, size: 5000 });
    db.kap.insert({ foo: 1 })

    // going back to empty on capped is a special case and must be tested
    db.createCollection("kap2", { capped: true, size: 5501 });
}

/* these writes on one primary only and will be rolled back. */
function doItemsToRollBack(db) {
    t = db.bar;
    t.insert({ q: 4 });
    t.update({ q: 3 }, { q: 3, rb: true });

    t.remove({ q: 40 }); // multi remove test

    t.update({ q: 2 }, { q: 39, rb: true });

    // rolling back a delete will involve reinserting the item(s)
    t.remove({ q: 1 });

    t.update({ q: 0 }, { $inc: { y: 1} });

    db.kap.insert({ foo: 2 })
    db.kap2.insert({ foo: 2 })

    // create a collection (need to roll back the whole thing)
    //db.newcoll.insert({ a: true });

    // create a new empty collection (need to roll back the whole thing)
    //db.createCollection("abc");
}

function doWritesToKeep2(db) {
    t = db.bar;
    t.insert({ txt: 'foo' });
    t.remove({ q: 70 });
    t.update({ q: 0 }, { $inc: { y: 33} });
}

function verify(db) {
    print("verify");
    t = db.bar;
    assert(t.find({ q: 1 }).count() == 1);
    assert(t.find({ txt: 'foo' }).count() == 1);
    assert(t.find({ q: 4 }).count() == 0);
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
    a_conn = (master == conns[0]) ? conns[0] : conns[1];
    A = a_conn.getDB("admin");
    b_conn = (master == conns[0]) ? conns[1] : conns[0];
    a_conn.setSlaveOk();
    b_conn.setSlaveOk();
    B = b_conn.getDB("admin");
    assert(a_conn == master);

    //deb(master);

    // Make sure we have an arbiter
    assert.soon(function () {
        res = conns[2].getDB("admin").runCommand({ replSetGetStatus: 1 });
        return res.myState == 7;
    }, "Arbiter failed to initialize.");

    // Wait for initial replication
    var a = a_conn.getDB("foo");
    var b = b_conn.getDB("foo");

    doInitialWrites(a);
	replTest.awaitReplication();

    master = replTest.getMaster();
	print("disconnect primary from everywhere");
	replTest.partition(0,1);
	replTest.partition(0,2);
	// do some writes to a before it realizes it can no longer be primary
	print("do Items to Rollback");
    doItemsToRollBack(a);
	// wait for b to become master
	print("wait for B to become master");
    wait(function () { return B.isMaster().ismaster; });
	// do some writes to b, as it is the new master
	print("new writes on b");
    doWritesToKeep2(b);
	// now bring a back
	print("connect A back");
	replTest.unPartition(0,1);
	replTest.unPartition(0,2);
    sleep(5000);

    // everyone is up here...
    replTest.awaitReplication();
    assert( dbs_match(a,b), "server data sets do not match after rollback, something is wrong");

    pause("rollback2.js SUCCESS");
    replTest.stopSet(signal);
};

var reconnect = function(a,b) {
  wait(function() { 
      try {
        a.bar.stats();
        b.bar.stats();
        return true;
      } catch(e) {
        print(e);
        return false;
      }
    });
};

print("rollback2.js");

doTest( 15 );
