var testName = 'txn_rollback_create_database';

var path = MongoRunner.toRealDir(testName);
var port = allocatePorts(1, parseInt(myPort(), 10) + 1)[0];
var mongod = startMongod('--port', port,
                         '--dbpath', path,
                         '--nohttpinterface',
                         '--bind_ip', '127.0.0.1');

var db = mongod.getDB(testName);
db.foo.insert([{_id:1}, {_id:1}]);
assert(db.getLastError());
assert.eq(0, db.foo.count());
db.foo.insert({});
assert(!db.getLastError());
assert.eq(1, db.foo.count());

stopMongod(port);

mongod = startMongodNoReset('--port', port,
                            '--dbpath', path,
                            '--nohttpinterface',
                            '--bind_ip', '127.0.0.1');

db = mongod.getDB(testName);
assert.eq(1, db.foo.count());
