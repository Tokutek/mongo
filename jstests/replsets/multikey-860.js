
// test that making a key become multikey does not cause an exception to be thrown
// on the secondary (due to not having a write lock)

doTest = function (signal, startPort, txnLimit) {

    replTest = new ReplSetTest({ name: 'unicomplex', nodes: 3, startPort:startPort, txnMemLimit: txnLimit});
    var nodes = replTest.nodeList();

    var conns = replTest.startSet();
        var r = replTest.initiate({ "_id": "unicomplex",
                                  "members": [
                                              { "_id": 0, "host": nodes[0], priority:10  },
                                              { "_id": 1, "host": nodes[1]},
                                              { "_id": 2, "host": nodes[2], arbiterOnly: true}]
                              });

    // Make sure we have a master
    var master = replTest.getMaster();
    x = master.getDB("foo");
    x.createCollection("foo");
    x.foo.ensureIndex({tags:1});
    x.createCollection("bar");
    x.bar.ensureIndex({tags:1});

    // lets do an update that sets an index to multikey
    x.foo.insert({_id:0, tags:0});
    x.foo.update({_id:0},{tags : [1,2,3]});
    replTest.awaitReplication();
    // now lets do an insert that sets an index to multikey
    x.bar.insert({tags : [1,2,3]});
    replTest.awaitReplication();
    x.foo.drop();
    x.bar.drop();
    replTest.awaitReplication();


    x.createCollection("foo", {capped:true, size:1024});
    x.foo.ensureIndex({tags:1});
    x.createCollection("bar", {capped:true, size:1024});
    x.bar.ensureIndex({tags:1});

    // lets do an update that sets an index to multikey
    x.foo.insert({_id:0, tags:0});
    x.foo.update({_id:0},{tags : [1,2,3]});
    replTest.awaitReplication();
    // now lets do an insert that sets an index to multikey
    x.bar.insert({tags : [1,2,3]});
    replTest.awaitReplication();


    replTest.stopSet(signal);
}
doTest(15, 31000, 1000000);

