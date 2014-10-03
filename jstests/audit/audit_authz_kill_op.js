// test that authzKillOp gets called.

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_authz_kill_op';

auditTest(
    'authzKillOp',
    function(m) {
        createAdminUserForAudit(m);
        var testDB = m.getDB(testDBName);
        var user = createNoPermissionUserForAudit(m, testDB);

        // Admin should be allowed to perform the operation.
        // NOTE: We expect NOT to see an audit event
        // when an 'admin' user performs this operation.
        var adminDB = m.getDB('admin');
        adminDB.auth('admin','admin');
        var operation = testDB.currentOp(true);
        assert.eq(null, testDB.getLastError());
        var first = operation.inprog[0];
        var id = first.opid;
        adminDB.logout();

        // User (tom) with no permissions logs in.
        var r = testDB.auth('tom', 'tom');
        assert(r);

        // Tom tries to kill this process.
        testDB.killOp(id);

        // Tom logs out.
        testDB.logout();

        // Verify that audit event was inserted.
        auditColl = getAuditEventsCollection(m, undefined, true);
        assert.eq(1, auditColl.count({
            atype: "authCheck",
            ts: withinTheLastFewSeconds(),
            'params.command': 'killOp',
            result: 13, // <-- Unauthorized error, see error_codes.err...
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { auth:"" }
);
