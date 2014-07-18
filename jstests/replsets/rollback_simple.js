// simple test of rollback from the 1.0 days, refactored

var filename;
//if (TestData.testDir !== undefined) {
//    load(TestData.testDir + "/replsets/_rollback_helpers.js");
//} else {
    load('jstests/replsets/_rollback_helpers.js');
//}

function doInitialWrites(conn) {
    var t = conn.getDB("test").bar;
    t.insert({ q:0});
    t.insert({ q: 1, a: "foo" });
    t.insert({ q: 2, a: "foo", x: 1 });
    t.insert({ q: 3, bb: 9, a: "foo" });
    t.insert({ q: 40, a: 1 });
    t.insert({ q: 40, a: 2 });
    t.insert({ q: 70, txt: 'willremove' });
    var db = conn.getDB("test");
    db.createCollection("kap", { capped: true, size: 5000 });
    db.kap.insert({ foo: 1 })

    // going back to empty on capped is a special case and must be tested
    assert.commandWorked(db.createCollection("kap2", { capped: true, size: 5501 }));

    db.createCollection("foo");
}

function doItemsToRollBack(conn) {
    db = conn.getDB("test");
    t = db.bar;
    t.insert({ q: 4 });
    t.update({ q: 3 }, { q: 3, rb: true });

    t.remove({ q: 40 }); // multi remove test

    t.update({ q: 2 }, { q: 39, rb: true });

    // rolling back a delete will involve reinserting the item(s)
    t.remove({ q: 1 });

    t.update({ q: 0 }, { $inc: { y: 1} });

    db.kap.insert({ foo: 2 })
    db.kap2.insert({ foo: 2 })

    db.runCommand("beginTransaction");
    t.insert({q:3});
    t.update({ q: 3 }, { q: 4, rb: true });
    db.runCommand("commitTransaction");
}

preloadLotsMoreData = function(conn) {
    var res = conn.getDB("admin").runCommand({configureFailPoint: 'disableReplInfoThread', mode: 'alwaysOn'});
    assert.eq(1, res.ok, "could not disable repl info thread");

    for (var i = 10; i < 200; i++) {
        conn.getDB("test").foo.insert({_id : i, state : "after split"});
    }
};

doRollbackTest( 15, 1000000, 31000, doInitialWrites, preloadLotsMoreData, doItemsToRollBack, false );
doRollbackTest( 15, 1, 41000, doInitialWrites, preloadLotsMoreData, doItemsToRollBack, false );

