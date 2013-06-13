// Test that incorrect protocol versions are rejected when trying to create a replica set.

function configFor(rs, pv) {
    var config = rs.getReplSetConfig();
    if (pv !== undefined) {
        config.protocolVersion = pv;
    }
    return config;
}

function test(pv, expectSuccess) {
    var rs = new ReplSetTest({"name": "protocol_version", "nodes": 3});
    rs.startSet();
    var config = configFor(rs, pv);
    var master = rs.nodes[0];
    var res = master.getDB("admin").runCommand({replSetInitiate: config});
    if (expectSuccess === false) {
        assert.commandFailed(res, "initiate with version " + pv + " should have failed");
    }
    else {
        assert.commandWorked(res, "initiate with version " + pv + " should have worked");

        // Now try adding a normal node.
        var newNode = rs.add();
        var c = master.getDB("local")['system.replset'].findOne();
        var config = configFor(rs, pv);
        config.version = c.version + 1;
        rs.initiate(config, 'replSetReconfig');

        // Now try adding an arbiter.
        var newArbiter = rs.add();
        var c = master.getDB("local")['system.replset'].findOne();
        var config = configFor(rs, pv);
        config.version = c.version + 1;
        config.members[config.members.length - 1].arbiterOnly = true;
        rs.initiate(config, 'replSetReconfig');
    }
    rs.stopSet();
};

test();
test(0, false);
test(1, false);
test(2, false);
test(64, false);
test(65, true);
test(66, false);
