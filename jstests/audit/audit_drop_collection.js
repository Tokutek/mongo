// test that dropCollection gets audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_drop_collection';

auditTest(
    'dropCollection',
    function(m) {
        testDB = m.getDB(testDBName);
        var collName = 'foo';
        var coll = testDB.getCollection(collName);
        coll.insert({ a: 17 });
        assert.eq(null, testDB.getLastError());
        coll.drop();
        assert.eq(null, testDB.getLastError());

        var auditColl = getAuditEventsCollection(m);
        assert.eq(1, auditColl.count({
            atype: "dropCollection",
            ts: withinTheLastFewSeconds(),
            'params.ns': testDBName + '.' + collName,
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { /* no special mongod options */ }
);
