// test that enableSharding gets audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

auditTestShard(
    'enableSharding',
    function(st) {
        testDB = st.s0.getDB(jsTestName());
        testDB.dropDatabase();
        assert.eq(null, testDB.getLastError());
        assert.commandWorked(st.s0.adminCommand({enableSharding: jsTestName()}));

        auditColl = loadAuditEventsIntoCollection(st.s0, '/data/db/auditLog-s0.json', testDB.getName(), 'auditEvents');
        assert.eq(1, auditColl.count({
            atype: "enableSharding",
            ts: withinTheLastFewSeconds(),
            'params.ns': jsTestName(),
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { /* no special mongod options */ }
);
