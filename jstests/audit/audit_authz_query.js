// test that authzQuery gets called.

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_authz_query';

auditTest(
    'authzQuery',
    function(m) {
        createAdminUserForAudit(m);
        var testDB = m.getDB(testDBName);
        var user = createNoPermissionUserForAudit(m, testDB);

        // Admin should be allowed to perform the operation.
        // NOTE: We expect NOT to see an audit event
        // when an 'admin' user performs this operation.
        var adminDB = m.getDB('admin');
        adminDB.auth('admin','admin');
        testDB.foo.insert({'_id':1});
        assert.eq(null, testDB.getLastError());
        adminDB.logout();

        // User (tom) with no permissions logs in.
        var r = testDB.auth('tom', 'tom');
        assert(r);

        // Tom tries to perform a query, but will only
        // fail when he accecsses the document in the
        // returned cursor.  This will throw, so we 
        // have to ignore that exception in this test.
        var cursor = testDB.foo.find( {_id:1} );
        assert.eq(null, testDB.getLastError());
        var ok = true;
        try {
            cursor.next();
            ok = false;
        } catch (o) { }

        assert(ok);
        assert.neq(null, testDB.getLastError());

        // Tom logs out.
        testDB.logout();

        // Verify that audit event was inserted.
        auditColl = getAuditEventsCollection(m, undefined, true);
        assert.eq(1, auditColl.count({
            atype: "authCheck",
            ts: withinTheLastFewSeconds(),
            'params.ns': testDBName + '.' + 'foo',
            'params.command': 'query',
            result: 13, // <-- Unauthorized error, see error_codes.err...
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { auth:"" }
);
