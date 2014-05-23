// we support the _collectionExists command so that replication can periodically check whether some collections exist, during an initial sync
// this is a unit test for that command

var odb = db.getSiblingDB('collections_exist_cmd');
odb.dropDatabase();

odb.foo.insert({});
odb.bar.insert({});

// Test that command works normally
assert.commandWorked(odb.runCommand({'_collectionsExist': ['collections_exist_cmd.foo', 'collections_exist_cmd.bar']}));

// Test that it detects the first collection having been dropped
odb.runCommand('beginTransaction');
dropper = startParallelShell('var odb = db.getSiblingDB("collections_exist_cmd"); sleep(1000); odb.foo.drop();');
assert.commandWorked(odb.runCommand({'_collectionsExist': ['collections_exist_cmd.foo', 'collections_exist_cmd.bar']}));
dropper();
assert.commandFailed(odb.runCommand({'_collectionsExist': ['collections_exist_cmd.foo', 'collections_exist_cmd.bar']}));
odb.runCommand('rollbackTransaction');

// recreate foo
odb.foo.insert({});

// Test that it detects the second collection having been dropped
odb.runCommand('beginTransaction');
dropper = startParallelShell('var odb = db.getSiblingDB("collections_exist_cmd"); sleep(1000); odb.bar.drop();');
assert.commandWorked(odb.runCommand({'_collectionsExist': ['collections_exist_cmd.foo', 'collections_exist_cmd.bar']}));
dropper();
assert.commandFailed(odb.runCommand({'_collectionsExist': ['collections_exist_cmd.foo', 'collections_exist_cmd.bar']}));
odb.runCommand('rollbackTransaction');

// recreate bar
odb.bar.insert({});

// Test that it detects a collection being dropped and recreated
odb.runCommand('beginTransaction');
dropper = startParallelShell('var odb = db.getSiblingDB("collections_exist_cmd"); sleep(1000); odb.foo.drop(); odb.foo.insert({});');
assert.commandWorked(odb.runCommand({'_collectionsExist': ['collections_exist_cmd.foo', 'collections_exist_cmd.bar']}));
dropper();
assert.commandFailed(odb.runCommand({'_collectionsExist': ['collections_exist_cmd.foo', 'collections_exist_cmd.bar']}));
odb.runCommand('rollbackTransaction');

// now do some tests with partitioned collections
odb.bar.drop();
odb.createCollection("bar", {partitioned:1});
assert.commandWorked(odb.bar.addPartition({_id:10}));
assert.commandWorked(odb.bar.addPartition({_id:20}));
odb.beginTransaction();
dropper = startParallelShell('var odb = db.getSiblingDB("collections_exist_cmd"); sleep(1000); assert.commandWorked(odb.bar.addPartition({_id:30})); ');
assert.commandWorked(odb.runCommand({'_collectionsExist': ['collections_exist_cmd.bar']}));
dropper();
sleep(2000);
assert.commandFailed(odb.runCommand({'_collectionsExist': ['collections_exist_cmd.bar']}));
dropper = startParallelShell('var odb = db.getSiblingDB("collections_exist_cmd"); sleep(1000); assert.commandWorked(odb.bar.dropPartition(0)); ');
dropper();
sleep(2000);
assert.commandFailed(odb.runCommand({'_collectionsExist': ['collections_exist_cmd.bar']}));
dropper = startParallelShell('var odb = db.getSiblingDB("collections_exist_cmd"); sleep(1000); assert.commandWorked(odb.bar.dropPartition(1)); ');
dropper();
sleep(2000);
assert.commandFailed(odb.runCommand({'_collectionsExist': ['collections_exist_cmd.bar']}));

odb.dropDatabase();
