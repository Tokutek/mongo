// test that renameCollection gets audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_rename_collection';

auditTest(
    'renameCollection',
    function(m) {
        testDB = m.getDB(testDBName);
        testDB.dropDatabase();
        assert.eq(null, testDB.getLastError());

        var oldName = 'john';
        var newName = 'christian';

        testDB.createCollection(oldName);
        assert.eq(null, testDB.getLastError());
        testDB.getCollection(oldName).renameCollection(newName);
        assert.eq(null, testDB.getLastError());

        var auditColl = getAuditEventsCollection(m);
        var checkAuditLogForSingleRename = function() {
            assert.eq(1, auditColl.count({
                atype: "renameCollection",
                ts: withinTheLastFewSeconds(),
                'params.old': testDBName + '.' + oldName,
                'params.new': testDBName + '.' + newName,
                result: 0,
            }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
        }
        checkAuditLogForSingleRename();

        testDB.getCollection(oldName).renameCollection(newName);
        assert.neq(null, testDB.getLastError());

        // Second rename won't be audited because it did not succeed.
        checkAuditLogForSingleRename();
    },
    { /* no special mongod options */ }
);
