// test that replSetReconfig gets audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
    load(TestData.testDir + '/replsets/rslib.js');
} else {
    load('jstests/audit/_audit_helpers.js');
    load('jstests/replsets/rslib.js');
}

var testDBName = 'audit_create_collection';

auditTestRepl(
    'replSetReconfig',
    function(replTest) {
        var oldConfig = replTest.getReplSetConfig();
        var newConfig = JSON.parse(JSON.stringify(oldConfig));
        newConfig.version = 200; // tired of playing games with the version

        var master = replTest.getMaster();
        try {
            assert.commandWorked(master.adminCommand({ replSetReconfig: newConfig }));
        } catch (e) {
            print('caught exception ' + e + ' while running reconfig, checking audit logs anyway..');
        }

        reconfig(replTest, newConfig);
        // MAGIC MAGIC MAGIC MAGIC!
        sleep(5000);

        // Ensure that the reconfig audit event got logged on every member.
        withinTheLast20Seconds = withinTheLastFewSeconds(20);
        replTest.nodes.forEach(function(m) { 
            print('audit check looking for old, new: ' +tojson(oldConfig)+', '+tojson(newConfig));
            // We need to import the audit events collection into the master node.
            auditColl = getAuditEventsCollection(m, replTest.getMaster());
            assert.eq(1, auditColl.count({
                atype: "replSetReconfig",
                // Allow timestamps up to 20 seconds old, since replSetReconfig may be slow
                ts: withinTheLast20Seconds,
                // old version is not set, so we do not query for it here
                'params.old._id': oldConfig._id,
                'params.old.version': 1,
                'params.new._id': newConfig._id,
                'params.new.version': 200,
                result: 0,
            }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
        });
    },
    { /* no special mongod options */ }
);
