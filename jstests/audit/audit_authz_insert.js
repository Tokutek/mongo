// test that authzInsert gets called.

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_authz_insert';

auditTest(
    'authzInsert',
    function(m) {
        createAdminUserForAudit(m);
        var testDB = m.getDB(testDBName);
        var user = createNoPermissionUserForAudit(m, testDB);

        // Admin should be allowed to perform the operation.
        // NOTE: We expect NOT to see an audit event
        // when an 'admin' user performs this operation.
        var adminDB = m.getDB('admin');
        adminDB.auth('admin','admin');
        testDB.foo.insert({'_id': 1});
        assert.eq(null, testDB.getLastError());
        adminDB.logout();

        // User with no permissions logs in.
        testDB.auth('tom', 'tom');
        
        // Tom inserts data.
        testDB.foo.insert({'_id': 2});
        assert.neq(null, testDB.getLastError());

        // Tom logs out.
        testDB.logout();

        // Verify that audit event was inserted.
        auditColl = getAuditEventsCollection(m, undefined, true);
        assert.eq(1, auditColl.count({
            atype: "authCheck",
            ts: withinTheLastFewSeconds(),
            users: [ { user:'tom', db:testDBName} ],
            'params.ns': testDBName + '.' + 'foo',
            'params.command': 'insert',
            result: 13, // <-- Unauthorized error, see error_codes.err...
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { auth:"" }
);
