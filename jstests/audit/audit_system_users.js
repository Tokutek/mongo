// test that system.users writes get audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_create_drop_update_user';

auditTest(
    '{create/drop/update}User',
    function(m) {
        testDB = m.getDB(testDBName);

        var userObj = { user: 'john', pwd: 'john', roles: [ 'userAdmin' ] };
        testDB.addUser(userObj);
        assert.eq(null, testDB.getLastError());

        var auditColl = getAuditEventsCollection(m);
        assert.eq(1, auditColl.count({
            atype: "createUser",
            ts: withinTheLastFewSeconds(),
            'params.db': testDBName,
            'params.userObj.user': userObj.user,
            'params.userObj.roles': userObj.roles,
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));

        var updateObj = { $push: { roles: 'dbAdmin' } }
        testDB.system.users.update({ user: userObj.user }, updateObj);
        assert.eq(null, testDB.getLastError());
        assert.eq(1, testDB.system.users.count({ user: userObj.user, roles: [ 'userAdmin', 'dbAdmin' ] }),
                     "system.users update did not update role for user:" + userObj.user);
        auditColl = getAuditEventsCollection(m);
        assert.eq(1, auditColl.count({
            atype: "updateUser",
            ts: withinTheLastFewSeconds(),
            'params.db': testDBName,
            'params.pattern.user': userObj.user,
            // cannot actually query for the updateobj in a sane way, :(
            'params.multi': false,
            'params.upsert': false,
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));

        testDB.removeUser(userObj.user);
        assert.eq(null, testDB.getLastError());
        assert.eq(0, testDB.system.users.count({ user: userObj.user }),
                     "removeUser did not remove user:" + userObj.user);
        auditColl = getAuditEventsCollection(m);
        assert.eq(1, auditColl.count({
            atype: "dropUser",
            ts: withinTheLastFewSeconds(),
            'params.db': testDBName,
            'params.pattern.user': userObj.user,
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { /* no special mongod options */ }
);
