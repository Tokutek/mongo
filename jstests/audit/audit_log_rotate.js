// test that log rotate actually rotates the audit log

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var logDir = '/data/db';
var testDBName = jsTestName();

var getRotatedLogFilePaths = function(auditPath) {
    return ls(logDir).filter(function (path) {
        return path != auditPath && path.indexOf(logDir + '/auditLog.json') != -1;
    });
}

auditTest(
    'logRotate',
    function(m) {
        var auditOptions = m.adminCommand({ auditGetOptions: 1 });
        var auditPath = auditOptions.path;
        assert.eq(true, auditPath == logDir + '/auditLog.json',
                  "test assumption failure: auditPath is not logDir + auditLog.json? " +
                  auditPath);

        // Remove the audit log that got rotated on startup
        getRotatedLogFilePaths(auditPath).forEach(function (f) { removeFile(f) });

        // This should generate a few new audit log entries on ns 'test.foo'
        testDB = m.getDB(testDBName);
        testDB.createCollection('foo');
        assert.eq(null, testDB.getLastError());
        testDB.getCollection('foo').drop();
        assert.eq(null, testDB.getLastError());

        // There should be something in the audit log since we created 'test.foo'
        assert.neq(0, getAuditEventsCollection(m).count(),
                   "strange: no audit events before rotate.");

        // Rotate the server log. The audit log rotates with it.
        // Once rotated, the audit log should be empty.
        assert.commandWorked(m.getDB('admin').runCommand({ logRotate: 1 }));
        var auditLogAfterRotate = getAuditEventsCollection(m).find({ 
            // skip audit events that will be triggered by getAuditEventsCollection itself
            $not: { 'params.ns': 'local.auditCollection' }
        }).toArray();
        assert.eq(0, auditLogAfterRotate.length,
                  "Audit log has old events after rotate: " + tojson(auditLogAfterRotate));

        // Verify that the old audit log got rotated properly.
        var rotatedLogPaths = getRotatedLogFilePaths(auditPath);
        assert.eq(1, rotatedLogPaths.length,
                  "did not get exactly 1 rotated log file: " + rotatedLogPaths);

        // Verify that the rotated audit log has the same number of
        // log lines as it did before it got rotated.
        var rotatedLog = rotatedLogPaths[0];
        var countAfterRotate = cat(rotatedLog).split('\n').filter(function(line) { return line != "" }).length;
        assert.neq(0, countAfterRotate, "rotated log file was empty");
    },
    // Need to enable the logging manager by passing `logpath'
    { logpath: logDir + '/server.log' }
);
