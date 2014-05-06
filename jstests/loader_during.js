// Test that the rules are properly enforced during load.

var filename;
if (TestData.testDir !== undefined) {
    load(TestData.testDir + "/_loader_helpers.js");
} else {
    load('jstests/_loader_helpers.js');
}

var testTypicalLoad = function() {
    t = db.loadertypical;
    t.drop();
    begin();
    beginLoad('loadertypical', [ ], { });
    for (i = 0; i < 1000; i++) {
        t.insert({ _id: i, a: 1 });
    }
    commitLoad();
    commit();
    assert.eq(1000, t.count({ _id: { $gte: 0, $lte: 1000} }));
    assert.eq(1000, t.count({ a: 1}));
}();

var testTypicalLoadWithIndex = function() {
    t = db.loadertypical;
    t.drop();
    begin();
    beginLoad('loadertypical', [ { key: { a: 1 }, ns: db.getName() + '.loadertypical', name: 'a_1' } ], { });
    for (i = 0; i < 1000; i++) {
        t.insert({ _id: i, a: 1 });
    }
    commitLoad();
    commit();
    assert.eq(1000, t.count({ _id: { $gte: 0, $lte: 1000} }));
    assert.eq(1000, t.count({ a: 1}));
}();

var testOperationsGetRejected = function() {

    print('Testing operations that should get rejected');
    
    // It's hard to generalize the following tests because different
    // operations are expected to fail in different ways.
    // One example is that a failed count throws in the shell, while
    // a failed insert sets lastError.
    var doTest = function(whenUs, whenOther) {
        ['us', 'other'].forEach( function(who) {
            t = db.loaderclientreject;
            t2 = db.loaderclientreject2;
            t.drop();

            // make sure system.indexes and system.namespaces exist,
            // otherwise this multi-statement transaction will have
            // those collections table locked and others unable to insert.
            t2.insert({});
            t2.remove({});

            begin();
            beginLoad('loaderclientreject', [ { key: { zzz: 1 }, name: "zzz_1" } ], { });

            var commitAndCheck = function() {
                commitLoad();
                commit();
                assert.eq(0, t.count());
            };
            if (who == 'us') {
                whenUs(commitAndCheck);
            } else {
                whenOther(commitAndCheck);
            }
        });
    };

    // Find should return nothing for the caller and others.
    print('Testing find');
    doTest(
        function(commitContinuation) {
            n = db.loaderclientreject.find().itcount();
            assert.eq(0, n);
            commitContinuation();
        },
        function(commitContinuation) {
            s = startParallelShell('db.loaderclientreject.find().itcount(); assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            commitContinuation();
            assert.eq(0, t2.count()); // verified that the find() failed above
        }
    );

    // Count returns nothing for calling client, throws for other.
    print('Testing count');
    doTest(
        function(commitContinuation) {
            n = db.loaderclientreject.count();
            assert.eq(0, n);
            commitContinuation();
        },
        function(commitContinuation) {
            s = startParallelShell('db.loaderclientreject.count(); db.loaderclientreject2.insert({});'); s();
            commitContinuation();
            assert.eq(0, t2.count());
        }
    );

    // Insert is ok for the calling client, fails for other.
    print('Testing insert');
    doTest(
        function(commitContinuation) {
            // We won't test anything for inserting on the calling client.
            commitContinuation();
        },
        function(commitContinuation) {
            s = startParallelShell('db.loaderclientreject.insert({}); assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            commitContinuation();
            assert.eq(1, t2.count());
        }
    );

    // Update does nothing for the caller, fails for other clients.
    print('Testing update');
    doTest(
        function(commitContinuation) {
            db.loaderclientreject.update({}, {$set : {c:1 } }); // query will match nothing, no upsert, so no error
            assert(!db.getLastError());
            db.loaderclientreject.update({}, {$set : {c:1 } }, true, true);
            assert(db.getLastError());
            db.loaderclientreject.update({}, {z:1}, true, true);
            assert(db.getLastError());
            db.loaderclientreject.update({ _id: 0 }, {z:1}, true, true);
            commitContinuation();
        },
        function(commitContinuation) {
            s = startParallelShell('db.loaderclientreject.update({}, {$set : {c:1 } }); ' +
                                   'assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            s = startParallelShell('db.loaderclientreject.update({}, {$set : {c:1 } }, true, true); ' +
                                   'assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            s = startParallelShell('db.loaderclientreject.update({}, {z:1}, true, true); ' +
                                   'assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            s = startParallelShell('db.loaderclientreject.update({_id:0}, {z:1}, true, true); ' +
                                   'assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            commitContinuation();
            assert.eq(4, t2.count());
        }
    );

    // Remove matches nothing for the caller, so no error. Other clients won't be able to access the ns.
    print('Testing remove');
    doTest(
        function(commitContinuation) {
            db.loaderclientreject.remove({});
            assert(!db.getLastError());
            commitContinuation();
        },
        function(commitContinuation) {
            s = startParallelShell('db.loaderclientreject.remove({}); ' +
                                   'assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            commitContinuation();
            assert.eq(1, t2.count());
        }
    );

    // Drop/dropIndexes/dropDatabase should throw for the caller and other clients.
    print('Testing drop');
    doTest(
        function(commitContinuation) {
            assert.throws(function() { db.loaderclientreject.drop() });
            assert.throws(function() { db.loaderclientreject.dropIndexes() });
            db.dropDatabase();
            assert(db.getLastError());
            commitContinuation();
        },
        function(commitContinuation) {
            s = startParallelShell('db.loaderclientreject.drop(); ' +
                                   'assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            s = startParallelShell('db.loaderclientreject.dropIndexes(); ' +
                                   'assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            s = startParallelShell('db.dropDatabase(); ' +
                                   'assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            commitContinuation();
            // only dropdatabase does not throw, because it hits lock wait timeout on the system.namespaces table lock
            assert.eq(1, t2.count());
        }
    );

    print('Testing reIndex');
    doTest(
        function(commitContinuation) {
            assert.throws(db.loaderclientreject.reIndex());
            commitContinuation();
        },
        function(commitContinuation) {
            s = startParallelShell('db.loaderclientreject.reIndex(); ' +
                                   'assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            commitContinuation();
            assert.eq(1, t2.count());
        }
    );

    print('Testing ensureIndex');
    doTest(
        function(commitContinuation) {
            db.loaderclientreject.ensureIndex({ a: 1 });
            assert(db.getLastError());
            commitContinuation();
        },
        function(commitContinuation) {
            s = startParallelShell('db.loaderclientreject.ensureIndex(); ' +
                                   'assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            commitContinuation();
            assert.eq(1, t2.count());
        }
    );

    print('Testing renameCollection');
    doTest(
        function(commitContinuation) {
            db.loaderclientreject.renameCollection('somethingelse');
            assert(db.getLastError());
            commitContinuation();
        },
        function(commitContinuation) {
            s = startParallelShell('db.loaderclientreject.renameCollection("somethingelse"); ' +
                                   'assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            commitContinuation();
            assert.eq(1, t2.count());
        }
    );

    print('Testing stats');
    doTest(
        function(commitContinuation) {
            // Stats is ok for the caller.
            db.loaderclientreject.stats();
            assert(!db.getLastError());
            commitContinuation();
        },
        function(commitContinuation) {
            // Stats should fail for others because the ns is not accessible.
            s = startParallelShell('db.loaderclientreject.stats(); ' +
                                   'assert(db.getLastError()); db.loaderclientreject2.insert({});'); s();
            commitContinuation();
            assert.eq(1, t2.count());
        }
    );

}();
