// test that fast updates work like normal updates in the common case

fastcoll = db.updatefast;
nonfastcoll = db.updatefast2;

function checkUpdate(initialDocs, query, updateobj, options) {
    function withFastUpdates() {
        fastcoll.remove({});
        fastcoll.insert(initialDocs);
        assert.commandWorked(db.getSisterDB('admin').runCommand({ setParameter: 1, fastupdates: true }));
        fastcoll.update(query, updateobj, options);
        assert.commandWorked(db.getSisterDB('admin').runCommand({ setParameter: 1, fastupdates: false }));
        return fastcoll.find();
    }
    function withNoFastUpdates(fn) {
        nonfastcoll.remove({});
        nonfastcoll.insert(initialDocs);
        assert.commandWorked(db.getSisterDB('admin').runCommand({ setParameter: 1, fastupdates: false }));
        nonfastcoll.update(query, updateobj, options);
        return nonfastcoll.find();
    }
    fastResult = withFastUpdates();
    nonFastResult = withNoFastUpdates();
    print('checking initial: ' + tojson(initialDocs) + ' query: ' + tojson(query) + ' update: ' + tojson(updateobj) + ' options: ' + tojson(options));
    assert.eq(fastResult.toArray(), nonFastResult.toArray(), "update result differ");
}

[ { key: { _id: 1 }, options: { } },
  { key: { c: 1 }, options: { } },
  { key: { z: 1 }, options: { clustering: true } }
].forEach(function(withIndex) {
    [ { upsert: false }, { upsert: true } ].forEach(function(withUpdateOptions) {
        fastcoll.drop();
        nonfastcoll.drop();
        fastcoll.ensureIndex(withIndex.key, withIndex.options);
        nonfastcoll.ensureIndex(withIndex.key, withIndex.options);
        print('withIndex: ' + tojson(withIndex));
        checkUpdate([ { _id: 0 } ], { _id: 0 }, { $inc : { c: 1 } }, withUpdateOptions);
        if (withUpdateOptions.upsert) {
            // fastupdate will always upsert, so only run this test when upsert
            // is true and we expect both fast and nonfast to have the same result
            checkUpdate([ { _id: 0 } ], { _id: 1 }, { $inc : { c: 1 } }, withUpdateOptions);
        }
    });
});
