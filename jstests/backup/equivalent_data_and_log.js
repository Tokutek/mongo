var basedir = '/data/db/' + jsTestName();

// ensure basedir exists
resetDbpath(basedir);

function doTest(d, l) {
    var ports = allocatePorts(2);
    var m = MongoRunner.runMongod({port: ports[0], dbpath: basedir + d, logDir: basedir + l, v: 1});

    var db = m.getDB(jsTestName());
    db.foo.insert({dbpath: d});
    var gle = db.runCommand({getLastError: 1, j: true});
    assert.commandWorked(gle);
    assert(!gle.err);
    assert.eq(1, db.foo.count());

    var backupdir = basedir + '/backuproot';
    resetDbpath(backupdir);

    var admin = m.getDB('admin');
    assert.commandWorked(admin.runCommand({loadPlugin: 'backup_plugin'}));
    assert.commandWorked(admin.runCommand({backupStart: backupdir}));

    var m2 = MongoRunner.runMongod({port: ports[1], dbpath: backupdir, noCleanData: true});

    var db2 = m2.getDB(jsTestName());
    assert.eq(1, db2.foo.count());
    assert.eq(d, db2.foo.findOne().dbpath);

    MongoRunner.stopMongod(ports[1]);

    MongoRunner.stopMongod(ports[0]);
}

doTest('/data', '/data');
doTest('/data', '/data/');
doTest('/data/', '/data');
doTest('/data/a', '/data/a/../a');

// This is a little bit insane, but boost::filesystem::create_directory doesn't like creating
// /data/a/../a if /data/a doesn't exist yet.  Since runMongod uses that inside resetDbpath, we
// can't test the case where --dbpath has this strange form, unless we do something equally strange
// ourselves.  Maybe TODO

//doTest('/data/a/../a', '/data/a');
