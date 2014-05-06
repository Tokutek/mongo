// test that renaming a partitioned collection works as expected
tn = "partition_rename";
tn2 = "partition_rename2";
t = db[tn];
t2 = db[tn2];
t.drop();
t2.drop();

assert.commandWorked(db.createCollection(tn, {partitioned:1}));
t.insert({_id:1, a:2});
t.addPartition();
t.insert({_id:2, a:1});

// test a simple rename
admin = db.getMongo().getDB( "admin" );
assert.commandFailed(admin.runCommand({renameCollection:db.getName() + '.' + tn,to:"targetdb.mycol"}));

assert.commandWorked(t.renameCollection(tn2));
assert(t2.getIndexes().length == 1);
assert(t2.count() == 2);

// add an index
t2.ensureIndex({a:1});
e = db.getLastErrorObj();
assert(e.err == null);
assert(t2.getIndexes().length == 2);

// now do another rename
assert.commandWorked(t2.renameCollection(tn));
assert(t.getIndexes().length == 2);
assert(t.count() == 2);
t.drop();

