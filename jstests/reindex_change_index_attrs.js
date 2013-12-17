var collname = 'reindex_change_index_attrs';
t = db[collname];

var idxStats = function(stats, name) {
    for (var i = 0; i < stats.indexDetails.length; ++i) {
        if (stats.indexDetails[i].name == name) {
            return stats.indexDetails[i];
        }
    }
}

var test_id = function(idx) {
    t.drop();
    t.insert({});
    assert.eq(null, db.getLastError());
    var oldStats = idxStats(t.stats(), '_id_');
    var res = t.reIndex(idx, {compression: 'lzma', readPageSize: '32k'});
    assert.commandWorked(res);
    assert.eq(res.was.compression, oldStats.compression);
    assert.eq(res.was.readPageSize, oldStats.readPageSize);
    var newStats = idxStats(t.stats(), '_id_');
    assert.eq(newStats.compression, 'lzma');
    assert.eq(newStats.readPageSize, 32 * 1024);
    assert.eq(newStats.pageSize, oldStats.pageSize);
}
test_id('_id_');
test_id({_id: 1});

t.drop();
db.runCommand({create: collname, compression: 'quicklz'});
t.ensureIndex({i:1}, {compression: 'lzma', clustering: true});
var oldStats = t.stats();
assert.eq(idxStats(oldStats, '_id_').compression, 'quicklz');
assert.eq(idxStats(oldStats, 'i_1').compression, 'lzma');
var res = t.reIndex('*', {compression: 'zlib', pageSize: '8MB'});
assert.commandWorked(res);
for (var i = 0; i < res.was.length; ++i) {
    assert.eq(res.was[i].name, oldStats.indexDetails[i].name);
    assert.eq(res.was[i].compression, oldStats.indexDetails[i].compression);
    assert.eq(res.was[i].pageSize, oldStats.indexDetails[i].pageSize);
}
var newStats = t.stats();
for (var i = 0; i < newStats.indexDetails.length; ++i) {
    assert.eq(newStats.indexDetails[i].name, oldStats.indexDetails[i].name);
    assert.eq(newStats.indexDetails[i].compression, 'zlib');
    assert.eq(newStats.indexDetails[i].readPageSize, oldStats.indexDetails[i].readPageSize);
    assert.eq(newStats.indexDetails[i].pageSize, 8 * 1024 * 1024);
}
