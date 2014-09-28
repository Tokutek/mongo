// test that dropIndex gets audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_drop_index';

auditTest(
    'dropIndex',
    function(m) {
        testDB = m.getDB(testDBName);
        var collName = 'foo';
        var idxName = 'fooIdx';
        var coll = testDB.getCollection(collName);
        coll.ensureIndex({ a: 1 }, { name: idxName });
        assert.eq(null, testDB.getLastError());
        coll.dropIndex({ a: 1 });
        assert.eq(null, testDB.getLastError());

        var auditColl = getAuditEventsCollection(m);
        assert.eq(1, auditColl.count({
            atype: "dropIndex",
            ts: withinTheLastFewSeconds(),
            'params.ns': testDBName + '.' + collName,
            'params.indexName': idxName,
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { /* no special mongod options */ }
);
