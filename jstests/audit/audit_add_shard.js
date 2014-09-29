// test that enableSharding gets audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

auditTestShard(
    'addShard',
    function(st) {
        var port = allocatePorts(10)[9];
        var conn1 = MongoRunner.runMongod({dbpath: '/data/db/' + jsTestName() + '-extraShard-' + port, port: port});

        var hostandport = 'localhost' + ':' + port;
        assert.commandWorked(st.s0.adminCommand({addshard: hostandport}));

        auditColl = loadAuditEventsIntoCollection(st.s0, '/data/db/auditLog-s0.json', jsTestName(), 'auditEvents');
        assert.eq(1, auditColl.count({
            atype: "addShard",
            ts: withinTheLastFewSeconds(),
            'params.connectionString': hostandport,
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { /* no special mongod options */ }
);
