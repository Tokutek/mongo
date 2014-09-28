var auditPath = 'auditLog.json';

var auditTest = function(name, fn, serverParams) {
    var loudTestEcho = function(msg) {
        s = '----------------------------- AUDIT UNIT TEST: ' + msg + '-----------------------------';
        print(Array(s.length + 1).join('-'));
        print(s);
    }

    loudTestEcho(name + ' STARTING ');
    removeFile(auditPath);
    var dbpath = TestData.testDir !== undefined ? 
                 TestData.testDir : '/data/db';
    var port = allocatePorts(1);
    var startServer = function(extraParams) {
        params = Object.merge(serverParams, extraParams);
        return MongoRunner.runMongod(
            Object.merge({
                port: port,
                dbpath: dbpath,
                auditDestination: 'file',
                auditPath: auditPath,
                auditFormat: 'JSON'
            }, params)
        );
    }
    var stopServer = function() {
        MongoRunner.stopMongod(port);
    }
    var restartServer = function() {
        stopServer();
        return startServer({ noCleanData: true });
    }
    try {
        fn(startServer(), restartServer);
    } finally {
        MongoRunner.stopMongod(port);
    }
    loudTestEcho(name + ' PASSED ');
}

// Drop the existing audit events collection, import
// the audit json file, then return the new collection.
var getAuditEventsCollection = function(m) {
    // the audit log is specifically parsable by mongoimport,
    // so we use that to conveniently read its contents.
    var localDB = m.getDB('local');
    var auditCollectionName = 'auditCollection';
    runMongoProgram('mongoimport',
                    '--db', 'local',
                    '--collection', auditCollectionName,
                    '--drop',
                    '--host', localDB.hostInfo().system.hostname,
                    '--file', auditPath);

    // should get as many entries back as there are non-empty
    // strings in the audit log
    auditCollection = localDB.getCollection(auditCollectionName)
    assert.eq(auditCollection.count(),
              cat(auditPath).split('\n').filter(function(o) { return o != "" }).length,
              "getAuditEventsCollection has different count than the audit log length");

    // there should be no duplicate audit log lines
    auditCollection.ensureIndex(
        { atype: 1, ts: 1, local: 1, remote: 1, users: 1, params: 1, result: 1 },
        { unique: true }
    );
    assert.eq(null, localDB.getLastError());

    return auditCollection;
}

// Get a query that matches any timestamp generated in the last few (or n) seconds
var withinTheLastFewSeconds = function(n) {
    now = Date.now();
    fewSecondsAgo = now - ((n !== undefined ? n : 3) * 1000);
    return { '$gte' : new Date(fewSecondsAgo), '$lte': new Date(now) };
}
