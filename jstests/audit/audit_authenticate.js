// test that authenticate gets audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_authenticate';
auditTest(
    'authenticate',
    function(m) {
        var testDB = m.getDB(testDBName);
        testDB.addUser({ user: 'john', pwd: 'john', roles: [ 'userAdmin' ] });
        assert.eq(null, testDB.getLastError());

        testDB.auth('john', 'john');
        assert.eq(null, testDB.getLastError(), "could not auth as john (pwd john)");

        var auditColl = getAuditEventsCollection(m);
        assert.eq(1, auditColl.count({
            atype: 'authenticate',
            ts: withinTheLastFewSeconds(),
            'params.user': 'john',
            // The one and only mechanism as of audit v1
            'params.mechanism': 'MONGODB-CR',
            'params.db': testDBName,
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));

        ok = testDB.auth('john', 'nope');
        assert.eq(false, ok, "incorrectly able to auth as john (pwd nope)");

        // ErrorCodes::AuthenticationFailed in src/mongo/base/error_codes.err
        var authenticationFailureCode = 18;

        var auditColl = getAuditEventsCollection(m);
        assert.eq(1, auditColl.count({
            atype: 'authenticate',
            'params.user': 'john',
            // The one and only mechanism as of audit v1
            'params.mechanism': 'MONGODB-CR',
            'params.db': testDBName,
            ts: withinTheLastFewSeconds(),
            result: authenticationFailureCode,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    // Enable auth for this test
    { auth: "" }
);
