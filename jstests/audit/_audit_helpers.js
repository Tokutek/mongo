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
    var auditPath = dbpath + '/auditLog.json';
    removeFile(auditPath);
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

var auditTestRepl = function(name, fn, serverParams) {
    var loudTestEcho = function(msg) {
        s = '----------------------------- AUDIT REPL UNIT TEST: ' + msg + '-----------------------------';
        print(Array(s.length + 1).join('-'));
        print(s);
    }

    loudTestEcho(name + ' STARTING ');
    var replTest = new ReplSetTest({ name: 'auditTestReplSet', cleanData: true, nodes: 2 });
    replTest.startSet({ auditDestination: 'file' });
    var config = {
        _id: 'auditTestReplSet',
        members: [
            { _id: 0, host: getHostName() + ":" + replTest.ports[0], priority: 2 },
            { _id: 1, host: getHostName() + ":" + replTest.ports[1], priority: 1 },
        ]
    };
    replTest.initiate(config);
    fn(replTest);
    loudTestEcho(name + ' PASSED ');
}

var auditTestShard = function(name, fn, serverParams) {
    var loudTestEcho = function(msg) {
        s = '----------------------------- AUDIT SHARDED UNIT TEST: ' + msg + '-----------------------------';
        print(Array(s.length + 1).join('-'));
        print(s);
    }

    loudTestEcho(name + ' STARTING ');
    var st = new ShardingTest({ name: 'auditTestSharding',
                                verbose: 1,
                                mongos: [
                                    Object.merge({
                                        auditPath: '/data/db/auditLog-s0.json',
                                        auditDestination: 'file',
                                        auditFormat: 'JSON'
                                    }, serverParams),
                                    Object.merge({
                                        auditPath: '/data/db/auditLog-s1.json',
                                        auditDestination: 'file',
                                        auditFormat: 'JSON'
                                    }, serverParams),
                                ],
                                shards: 2,
                                config: 1,
                                other: {
                                    shardOptions: Object.merge({
                                        auditDestination: 'file',
                                        auditFormat: 'JSON'
                                    }, serverParams),
                                },
                              });
    try {
        fn(st);
    } finally {
        st.stop();
    }
    loudTestEcho(name + ' PASSED ');
}

// Drop the existing audit events collection, import
// the audit json file, then return the new collection.
var getAuditEventsCollection = function(m, primary, useAuth) {
    var auth = useAuth !== undefined ? true : false;
    if (auth) {
        var adminDB = m.getDB('admin');
        adminDB.auth('admin','admin');
        var localDB = m.getDB('local');
        localDB.auth('admin','admin');
    }

    // the audit log is specifically parsable by mongoimport,
    // so we use that to conveniently read its contents.
    var auditOptions = m.getDB('admin').runCommand('auditGetOptions');
    var auditPath = auditOptions.path;
    var auditCollectionName = 'auditCollection';
    return loadAuditEventsIntoCollection(m, auditPath, 'local', auditCollectionName, primary, auth);
}

// Load any file into a named collection.
var loadAuditEventsIntoCollection = function(m, filename, dbname, collname, primary, auth) {
    var db = primary !== undefined ? primary.getDB(dbname) : m.getDB(dbname);
    // the audit log is specifically parsable by mongoimport,
    // so we use that to conveniently read its contents.
    if (auth) {
        runMongoProgram('mongoimport',
                        '--username', 'admin',
                        '--password', 'admin',
                        '--authenticationDatabase', 'admin',
                        '--db', dbname,
                        '--collection', collname,
                        '--drop',
                        '--host', db.hostInfo().system.hostname,
                        '--file', filename);
    } else {
        runMongoProgram('mongoimport',
                        '--db', dbname,
                        '--collection', collname,
                        '--drop',
                        '--host', db.hostInfo().system.hostname,
                        '--file', filename);
    }

    // should get as many entries back as there are non-empty
    // strings in the audit log
    auditCollection = db.getCollection(collname)
    assert.eq(auditCollection.count(),
              cat(filename).split('\n').filter(function(o) { return o != "" }).length,
              "getAuditEventsCollection has different count than the audit log length");

    // there should be no duplicate audit log lines
    auditCollection.ensureIndex(
        { atype: 1, ts: 1, local: 1, remote: 1, users: 1, params: 1, result: 1 },
        { unique: true }
    );
    assert.eq(null, db.getLastError());

    return auditCollection;
}

// Get a query that matches any timestamp generated in the last few (or n) seconds
var withinTheLastFewSeconds = function(n) {
    now = Date.now();
    fewSecondsAgo = now - ((n !== undefined ? n : 3) * 1000);
    return { '$gte' : new Date(fewSecondsAgo), '$lte': new Date(now) };
}

// Create Admin user.  Used for authz tests.
var createAdminUserForAudit = function (m) {
    var adminDB = m.getDB('admin');
    adminDB.addUser( {'user':'admin', 'pwd':'admin', 'roles' : ['readWriteAnyDatabase',
                                                                'userAdminAnyDatabase',
                                                                'clusterAdmin']} );
}

// Creates a User with limited permissions. Used for authz tests.
var createNoPermissionUserForAudit = function (m, db) {
    var passwordUserNameUnion = 'tom';
    var adminDB = m.getDB('admin');
    adminDB.auth('admin','admin');
    db.addUser( {'user':'tom', 'pwd':'tom', 'roles':[]} );
    adminDB.logout();
    return passwordUserNameUnion;
}
