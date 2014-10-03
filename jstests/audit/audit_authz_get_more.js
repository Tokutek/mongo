// test that authzGetMore gets called.

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_authz_get_more';

auditTest(
    'authzGetMore',
    function(m) {
        createAdminUserForAudit(m);
        var testDB = m.getDB(testDBName);
        var user = createNoPermissionUserForAudit(m, testDB);

        // Admin should be allowed to perform the operation.
        // NOTE: We expect NOT to see an audit event
        // when an 'admin' user performs this operation.
        var adminDB = m.getDB('admin');
        adminDB.auth('admin','admin');

        // Insert lots of dummy data, if only to help ensure are data set is larger than the
        // returned batch set.
	var n = 1000;
        for (var i = 0; i < n; ++i) {
            testDB.foo.insert({'_id': i, s: 'lotsofdummydata'});
            assert.eq(null, testDB.getLastError());
        }

        // Using the admin user, get a bunch of batches, but not all of them.
        var myCursor = testDB.foo.find().batchSize(100);
	for (var i = 0; i < 100; i++) {
            printjson(myCursor.next());
            assert.eq(null, testDB.getLastError());
	}

        adminDB.logout();


        // User (tom) with no permissions logs in.
        var r = testDB.auth('tom', 'tom');
        assert(r);

        // Here, tom tries to use the cursor to get more data.  NOTE: mongo shell calls hasNext()
        // just before calling next().  Since Tom is not authorized for hasNext(), next() (and
        // getMore), the hasNext() call will throw.  We want to ignore that throw, and assert if it
        // does NOT throw.
        var ok = true;
        try {
            // if everything is good, this will throw!
            document = myCursor.next();
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
            users: [ { user:'tom', db:testDBName} ],
            'params.ns': testDBName + '.' + 'foo',
            'params.command': 'getMore',
            result: 13, // <-- Unauthorized error, see error_codes.err...
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { auth:"" }
);
