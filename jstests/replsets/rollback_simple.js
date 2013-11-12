// a test of rollback in replica sets
//
// try running as :
// 
//   mongo --nodb rollback2.js | tee out | grep -v ^m31
//


w = 0;

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

	db.runCommand("beginTransaction");
	t.insert({q:3});
    t.update({ q: 3 }, { q: 4, rb: true });
	db.runCommand("commitTransaction");
	

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

doTest = function (signal, txnMemLimit, startPort) {

    var num = 3;
    var host = getHostName();
    var name = "rollback_simple";
    var timeout = 60000;

    var replTest = new ReplSetTest( {name: name, nodes: num, startPort:startPort, txnMemLimit: txnMemLimit} );
    var conns = replTest.startSet();
    var port = replTest.ports;
    var config = {_id : name, members :
            [
             {_id:0, host : host+":"+port[0], priority:10 },
             {_id:1, host : host+":"+port[1]},
             {_id:2, host : host+":"+port[2], arbiterOnly : true},
            ],
             };

    replTest.initiate(config);
    replTest.awaitReplication();
    assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });

    // Make sure we have a master
    conns[0].setSlaveOk();
    conns[0].setSlaveOk();

    //deb(master);

    // Make sure we have an arbiter
    assert.soon(function () {
        res = conns[2].getDB("admin").runCommand({ replSetGetStatus: 1 });
        return res.myState == 7;
    }, "Arbiter failed to initialize.");

    // Wait for initial replication
    var a = conns[0].getDB("foo");
    var b = conns[1].getDB("foo");

    doInitialWrites(a);
    replTest.awaitReplication();

    print("shutting down conn 0");
    replTest.stop(0);
    print("waiting for conn 1 to become master");
    assert.soon(function() { return conns[1].getDB("admin").isMaster().ismaster; });

    print("do Items to Rollback");
    doItemsToRollBack(b);
    print("shutting down conn1");
    replTest.stop(1);


    print("shutting down conn1");
    replTest.stop(1);
    print("restarting conn0");
    replTest.restart(0);
    print("waiting for conn 0 to become master");
    assert.soon(function() { return conns[0].getDB("admin").isMaster().ismaster; });

    doWritesToKeep2(conns[0].getDB("foo"));
    for (i = 0; i < 1000; i++) {
        conns[0].getDB("foo").foo.insert({_id:i});
    }

    print("restarting conn1");
    replTest.restart(1);

    a = conns[0].getDB("foo");
    b = conns[1].getDB("foo");

    // everyone is up here...
    replTest.awaitReplication();
    assert( dbs_match(a,b), "server data sets do not match after rollback, something is wrong");

    print("rollback_simple.js SUCCESS");
    replTest.stopSet(signal);
};

print("rollback2.js");

doTest( 15, 1000000, 31000 );
doTest( 15, 0, 41000 );

