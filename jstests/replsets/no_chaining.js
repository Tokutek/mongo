
function myprint( x ) {
    print( "chaining output: " + x );
}

var replTest = new ReplSetTest({name: 'testSet', nodes: 3});
var nodes = replTest.startSet();
var hostnames = replTest.nodeList();
replTest.initiate(
    {
        "_id" : "testSet",
        "members" : [
            {"_id" : 0, "host" : hostnames[0]},
            {"_id" : 1, "host" : hostnames[1]},
            {"_id" : 2, "host" : hostnames[2], "priority" : 0}
        ],
        "settings" : {
            "chainingAllowed" : false
        }
    }
);

var master = replTest.getMaster();
replTest.awaitReplication();

var primaryIndex;
var secondaryIndex;

var breakNetwork = function() {
    replTest.bridge();
    master = replTest.getMaster();
    var x = master.getDB("admin").runCommand({replSetGetStatus:1});
    printjson(x);
    assert(x["members"][0]["state"] == 1 || x["members"][1]["state"] == 1);
    if (x["members"][0]["state"] == 1) {
        print("partitioning (0,2)");
        primaryIndex = 0;
        secondaryIndex = 1;
        replTest.partition(0, 2);
    }
    else {
        print("partitioning (1,2)");
        primaryIndex = 1;
        secondaryIndex = 0;
        replTest.partition(1, 2);
    }
};

var checkNoChaining = function() {
    master.getDB("test").foo.insert({x:1});

    assert.soon(
        function() {
            return nodes[1].getDB("test").foo.findOne() != null;
        }
    );
    assert.soon(
        function() {
            return nodes[0].getDB("test").foo.findOne() != null;
        }
    );

    var endTime = (new Date()).getTime()+10000;
    while ((new Date()).getTime() < endTime) {
        print('CHAINING IS NOT HAPPENING');
        assert(nodes[2].getDB("test").foo.findOne() == null,
               'Check that 2 does not catch up');
        sleep(500);
    }
};

var forceSync = function() {
    assert.soon(
        function() {
            var config = nodes[2].getDB("local").system.replset.findOne();
            var targetHost;
            if (secondaryIndex == 1) {
                targetHost = config.members[1].host;
            }
            else {
                targetHost = config.members[0].host;
            }
            printjson(nodes[2].getDB("admin").runCommand({replSetSyncFrom : targetHost}));
            return nodes[2].getDB("test").foo.findOne() != null;
        },
        'Check force sync still works'
    );
};

if (!_isWindows()) {
    print("break the network so that node 2 cannot replicate");
    breakNetwork();

    print("make sure chaining is not happening");
    checkNoChaining();

    print("check that forcing sync target still works");
    forceSync();

    var config = master.getDB("local").system.replset.findOne();
    assert.eq(false, config.settings.chainingAllowed, tojson(config));
}
