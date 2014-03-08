// cover all ways to run reIndex
var doTestWithIndexSpec = function(spec) {
    var rst = new ReplSetTest({name: 'reindex_persists_across_reboot', nodes: 1});

    var replicas = rst.startSet();
    rst.initiate();

    var db = rst.getMaster().getDB('test');
    db.foo.drop();
    db.foo.insert({});
    assert.eq('zlib', db.foo.stats().indexDetails[0].compression);
    assert.commandWorked(db.foo.reIndex(spec, {compression: 'quicklz'}));
    assert.eq('quicklz', db.foo.stats().indexDetails[0].compression);
    rst.restart(0, {}, true);
    db = rst.waitForMaster().getDB('test');
    assert.eq('quicklz', db.foo.stats().indexDetails[0].compression);

    function expectPartitionedCompression(comp, stats) {
        printjson(stats);
        assert.eq(comp, stats.indexDetails[0].compression);
        for (var i = 0; i < stats.partitions.length; ++i) {
            assert.eq(comp, stats.partitions[i].indexDetails[0].compression);
        }
    }

    var local = rst.getMaster().getDB('local');
    var oplogstats = local.oplog.rs.stats();
    expectPartitionedCompression('zlib', local.oplog.rs.stats());
    assert.commandWorked(local.oplog.rs.reIndex(spec, {compression: 'quicklz'}));
    expectPartitionedCompression('quicklz', local.oplog.rs.stats());
    assert.commandWorked(rst.getMaster().adminCommand('replAddPartition'));
    expectPartitionedCompression('quicklz', local.oplog.rs.stats());
    db.foo.insert({});
    rst.restart(0, {}, true);
    local = rst.waitForMaster().getDB('local');
    expectPartitionedCompression('quicklz', local.oplog.rs.stats());
    assert.commandWorked(rst.getMaster().adminCommand('replAddPartition'));
    expectPartitionedCompression('quicklz', local.oplog.rs.stats());

    rst.stopSet();
};

doTestWithIndexSpec('*');
doTestWithIndexSpec('_id_');
doTestWithIndexSpec({_id: 1});
