var ports = allocatePorts(2);
var m = MongoRunner.runMongod({port: ports[0], dbpath: '/data/db/backup_simple_data', v: 1});

var db = m.getDB(jsTestName());
db.foo.insert({simple: true});
var gle = db.runCommand({getLastError: 1, j: true});
assert.commandWorked(gle);
assert(!gle.err);
assert.eq(1, db.foo.count());

var backupdir = '/data/db/backup_simple_backup';
resetDbpath(backupdir);

var admin = m.getDB('admin');

assert.commandWorked(admin.runCommand({loadPlugin: 'backup_plugin'}));
assert.commandWorked(admin.runCommand({backupStart: backupdir}));

var m2 = MongoRunner.runMongod({port: ports[1], dbpath: backupdir, noCleanData: true});

var db2 = m2.getDB(jsTestName());
assert.eq(1, db2.foo.count());
assert.eq(true, db2.foo.findOne().simple);

MongoRunner.stopMongod(ports[1]);

MongoRunner.stopMongod(ports[0]);
