// test that fast updates work like normal updates in the common case

fastcollname = "updatefast";
fastcoll = db.updatefast;
nonfastcollname = "updatefast2"
nonfastcoll = db.updatefast2;


function changeFastUpdates(val){
        x = db.runCommand("isdbgrid");
        if (x.ok) {
            dbForParam = myShardingTest.getServer("test");
            assert.commandWorked(dbForParam.getDB('admin').runCommand({ setParameter: 1, fastupdates: val }));            
        }
        else {
            assert.commandWorked(db.getSisterDB('admin').runCommand({ setParameter: 1, fastupdates: val }));
        }
}

// make sure that the second test run is with fast updates disabled, because
// we don't want the mongod to keep fast updates enabled for tests run after this.
function checkUpdate(initialDocs, query, updateobj, options) {
    function withFastUpdates() {
        fastcoll.remove({});
        fastcoll.insert(initialDocs);
        changeFastUpdates(true);
        fastcoll.update(query, updateobj, options);
        changeFastUpdates(false);
        return fastcoll.find();
    }
    function withNoFastUpdates() {
        nonfastcoll.remove({});
        nonfastcoll.insert(initialDocs);
        changeFastUpdates(false);
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
        assert.commandWorked(db.createCollection(fastcollname));
        assert.commandWorked(db.createCollection(nonfastcollname));
        print("created collections");
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
