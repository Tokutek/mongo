var rst = new ReplSetTest({name: 'reindex_persists_across_reboot', nodes: 1});

var replicas = rst.startSet();
rst.initiate();

var db = rst.getMaster().getDB('test');
db.foo.drop();
db.foo.insert({});
assert.eq('zlib', db.foo.stats().indexDetails[0].compression);
assert.commandWorked(db.foo.reIndex('*', {compression: 'quicklz'}));
assert.eq('quicklz', db.foo.stats().indexDetails[0].compression);
rst.restart(0, {}, true);
db = rst.waitForMaster().getDB('test');
assert.eq('quicklz', db.foo.stats().indexDetails[0].compression);

rst.stopSet();
