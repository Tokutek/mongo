// dumprestore11.js

var t = new ToolTest( "dumprestore11" );

var maybetobytes = function(s) {
    var x = parseInt(s);
    if (isNaN(x)) {
        return s;
    }
    if (/kb?$/i.test(s)) {
        return x * 1024;
    } else if (/mb?$/i.test(s)) {
        return x * 1024 * 1024;
    } else if (/gb?$/i.test(s)) {
        return x * 1024 * 1024 * 1024;
    } else if (/tb?$/i.test(s)) {
        return x * 1024 * 1024 * 1024 * 1024;
    }
    return x;
};

assert.eqNoOrder = function(a, b, why) {
    // There really needs to be a better way to express this.
    // We want to consider {} and {options: {}} equal, which is slightly sloppy but harmless.
    if ((typeof a.options == 'undefined' && typeof b.options == 'object') ||
        (typeof a.options == 'object' && typeof b.options == 'undefined')) {
        return;
    }
    if (typeof a != "object" || typeof b != "object") {
        return assert.eq(a, b, why);
    }
    for (var x in a) {
        assert.eqNoOrder(a[x], b[x], why + ': comparing ' + tojson(a) + ' with ' + tojson(b));
    }
    for (var x in b) {
        assert.eqNoOrder(a[x], b[x], why + ': comparing ' + tojson(a) + ' with ' + tojson(b));
    }
};

var runTest = function(createFirst, createSecond, reIndexAll, pk, secondarySettable, secondaryUnsettable) {
    var defaults = {compression: 'zlib', pageSize: 4*1024*1024, readPageSize: 64*1024};
    var pkActual = Object.merge(defaults, pk);
    var secondaryActual = Object.merge(defaults, Object.merge(secondarySettable, secondaryUnsettable));

    var db = t.startDB();
    var t1 = db.t1;
    var t2 = db.t2;
    t1.drop();
    t2.drop();
    if (createFirst) {
        assert.commandWorked(db.runCommand(Object.extend({create: 't1'}, pk)));
    } else {
        t1.insert({});
    }
    t1.ensureIndex({i:1}, Object.merge(secondarySettable, secondaryUnsettable));
    if (createSecond) {
        assert.commandWorked(db.runCommand({create: 't2'}));
    } else {
        t2.insert({});
    }
    if (secondaryUnsettable) {
        t2.ensureIndex({i:1}, secondaryUnsettable);
    } else {
        t2.ensureIndex({i:1});
    }
    if (reIndexAll) {
        assert.eq(pk, secondarySettable);
        assert.commandWorked(db.t2.reIndex('*', pk));
    } else {
        assert.commandWorked(db.t2.reIndex({_id:1}, pk));
        assert.commandWorked(db.t2.reIndex({i:1}, secondarySettable));
    }

    var firstNamespaces = db.system.namespaces.find({name: t1.getFullName()}, {name: 0, 'options.create': 0}).toArray();
    var secondNamespaces = db.system.namespaces.find({name: t2.getFullName()}, {name: 0, 'options.create': 0}).toArray();
    for (var x in firstNamespaces) {
        assert.eqNoOrder(firstNamespaces[x], secondNamespaces[x], 'system.namespaces entries are unequal: ' + tojson(firstNamespaces) + ' != ' + tojson(secondNamespaces));
    }
    var firstIndexes = db.system.indexes.find({ns: t1.getFullName()}, {ns: 0}).sort({name:1}).toArray();
    var secondIndexes = db.system.indexes.find({ns: t2.getFullName()}, {ns: 0}).sort({name:1}).toArray()
    assert.eqNoOrder(firstIndexes, secondIndexes, 'system.indexes entries are unequal: ' + tojson(firstIndexes) + ' != ' + tojson(secondIndexes));

    t.runTool('dump', '--out', t.ext, '-d', db.getName());

    t1.drop();
    t2.drop();

    t.runTool('restore', '--dir', t.ext);

    var attrs = ['compression', 'pageSize', 'readPageSize'];
    var firstStats = t1.stats();
    var secondStats = t2.stats();
    attrs.forEach(function(attr) {
        assert.eq(firstStats.indexDetails[0][attr], maybetobytes(pkActual[attr]));
        assert.eq(firstStats.indexDetails[0][attr], secondStats.indexDetails[0][attr]);
        assert.eq(firstStats.indexDetails[1][attr], maybetobytes(secondaryActual[attr]));
        assert.eq(firstStats.indexDetails[1][attr], secondStats.indexDetails[1][attr]);
    });

    t.stop();
};

[false, true].forEach(function(createSecond) {
    runTest(false, createSecond, false, {}, {});
    runTest(false, createSecond, false, {}, {compression: 'quicklz', readPageSize: '32k'});
    runTest(true, createSecond, false, {compression: 'lzma', pageSize: '8M', readPageSize: '128k'}, {compression: 'lzma', pageSize: '8M', readPageSize: '128k'}, {clustering: true});
    runTest(true, createSecond, true, {compression: 'lzma', pageSize: '8M', readPageSize: '128k'}, {compression: 'lzma', pageSize: '8M', readPageSize: '128k'}, {clustering: true});
    runTest(true, createSecond, false, {compression: 'lzma', pageSize: '8M', readPageSize: '128k'}, {});
});
