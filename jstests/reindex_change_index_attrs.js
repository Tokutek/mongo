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
    printjson(oldStats);
    printjson(res);
    var checkWas = function(was) {
        assert.eq(was.compression, oldStats.compression);
        assert.eq(was.readPageSize, oldStats.readPageSize);
    };
    if (res.raw !== undefined) {
        for (var k in res.raw) {
            checkWas(res.raw[k].was);
        }
    } else {
        checkWas(res.was);
    }
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
var checkWas = function(was) {
    for (var i = 0; i < was.length; ++i) {
        assert.eq(was[i].name, oldStats.indexDetails[i].name);
        assert.eq(was[i].compression, oldStats.indexDetails[i].compression);
        assert.eq(was[i].pageSize, oldStats.indexDetails[i].pageSize);
    }
};
if (res.raw !== undefined) {
    for (var k in res.raw) {
        checkWas(res.raw[k].was);
    }
} else {
    checkWas(res.was);
}
var newStats = t.stats();
for (var i = 0; i < newStats.indexDetails.length; ++i) {
    assert.eq(newStats.indexDetails[i].name, oldStats.indexDetails[i].name);
    assert.eq(newStats.indexDetails[i].compression, 'zlib');
    assert.eq(newStats.indexDetails[i].readPageSize, oldStats.indexDetails[i].readPageSize);
    assert.eq(newStats.indexDetails[i].pageSize, 8 * 1024 * 1024);
}
