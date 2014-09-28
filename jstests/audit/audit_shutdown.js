// test that createColleciton gets audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

auditTest(
    'shutdown',
    function(m, restartServer) {
        m.getDB('admin').shutdownServer();
        m = restartServer();

        auditColl = getAuditEventsCollection(m);
        assert.eq(1, auditColl.count({
            atype: "shutdown",
            // Give 10 seconds of slack in case shutdown / restart was particularly slow
            ts: withinTheLastFewSeconds(10),
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { /* no special mongod options */ }
);
