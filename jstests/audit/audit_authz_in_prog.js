// test that authzInProg gets called.

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_authz_in_prog';

auditTest(
    'authzInProg',
    function(m) {
        createAdminUserForAudit(m);
        var testDB = m.getDB(testDBName);
        var user = createNoPermissionUserForAudit(m, testDB);

        // User (tom) with no permissions logs in.
        var r = testDB.auth('tom', 'tom');
        assert(r);

        // Tom tries to get the curret operations..
        var operation = testDB.currentOp(true);
        // NOTE: This doesn't seem to set the error message on the current db!?!

        // Tom logs out.
        testDB.logout();

        // Verify that audit event was inserted.
        auditColl = getAuditEventsCollection(m, undefined, true);
        assert.eq(1, auditColl.count({
            atype: "authCheck",
            ts: withinTheLastFewSeconds(),
            'params.command': 'inProg',
            result: 13, // <-- Unauthorized error, see error_codes.err...
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { auth:"" }
);
