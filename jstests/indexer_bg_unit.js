// test that various operations are prevented while a hot
// index is active and operating without a lock.

t = db.bg_idx_unit;
t.drop();

t.insert({ a: 1 });
t.insert({ b: 1 });
t.ensureIndex({ b: 1 });

var setFP = function(name, mode, info) {
    assert.commandWorked(
        db.adminCommand({ configureFailPoint: name, mode: mode, data: (info || {}) })
    );
}

var assertCommandFailsWithCode = function(res, code) {
    assert.eq(false, res.ok);
    assert.eq(code, res.code);
}

// causes hot indexes to sleep after beginning a hot index but before building it
// the sleep will persist until we activate the 'hotIndexSleepCond' fail point
setFP('hotIndexUnlockedBeforeBuild', 'alwaysOn', { sleep: true });

sh = startParallelShell('db.bg_idx_unit.ensureIndex({ a: 1 }, { background: true }); assert.eq(null, db.getLastError())');
assert.eq(2, t.stats().nindexes);
assert.soon(function() { return t.stats().nindexesbeingbuilt == 3 }); // the hot index client should begin shortly

// insert/update/delete/query ok
t.findOne();
assert.eq(null, db.getLastError());
t.insert({});
assert.eq(null, db.getLastError());
t.remove({ _id: 'nope' });
assert.eq(null, db.getLastError());
t.update({ _id: 'nope' }, { $inc: { a: 1 } });
assert.eq(null, db.getLastError());

print('test drop')
o = db.runCommand({ drop: t.getName() });
printjson(o);
assertCommandFailsWithCode(o, 16904); // can't drop with hot index

print('test drop db')
o = db.dropDatabase();
// fails with LockNotGranted at the moment, since drop database tries to
// delete from system.namespaces but the hot index thread has a row lock on the
// associated index's system.namespaces entry.
printjson(o);
assertCommandFailsWithCode(o, 16759); // can't drop db because you can't drop the collection, but see above

db.bad.insert({});

print('test rename (as target)')
o = db.adminCommand({ renameCollection: db.bad.getFullName(), to: t.getFullName()                        }) 
printjson(o);
assertCommandFailsWithCode(o, 10027); // target collection exists
o = db.adminCommand({ renameCollection: db.bad.getFullName(), to: t.getFullName(),      dropTarget: true })
printjson(o);
assertCommandFailsWithCode(o, 16904); // the drop fails

print('test rename (as source)')
o = db.adminCommand({ renameCollection: t.getFullName(),      to: db.getName() + '.nope',                  })
printjson(o);
assertCommandFailsWithCode(o, 17332); // can't rename collection with hot index
o = db.adminCommand({ renameCollection: t.getFullName(),      to: db.getName() + '.nope', dropTarget: true })
printjson(o);
assertCommandFailsWithCode(o, 17332); // can't rename collection with hot index
o = db.adminCommand({ renameCollection: t.getFullName(),      to: 'anotherDB.' + t.getName()             })
printjson(o);
assertCommandFailsWithCode(o, 17332); // can't rename collection with hot index

print('test reindex')
o = t.reIndex();
printjson(o);
assertCommandFailsWithCode(o, 17232); // can't rebuild indexes with hot index

// can't build indexes
print('test ensure index');
t.ensureIndex({ c: 1 });
assert.neq(null, db.getLastError());
t.ensureIndex({ c: 1 }, { background: true });
assert.neq(null, db.getLastError());

// can't make the other index multikey, but its ok to make a: 1 multikey
t.insert({ b: [ 1, 2 ] })
e = db.getLastError();
printjson(e);
assert(e.indexOf("cannot change the 'multikey' nature of an index") != -1);
t.insert({ a: [ 1, 2 ] })
assert.eq(null, db.getLastError());

print('turning on hotIndexSleepCond');
setFP('hotIndexSleepCond', 'alwaysOn'); // flip the switch

print('turning off hotIndexUnlockedBeforeBuild');
setFP('hotIndexUnlockedBeforeBuild', 'off');

print('turning off hotIndexSleepCond');
setFP('hotIndexSleepCond', 'off'); // then turn it back off again?

sh();
assert.eq(3, t.stats().nindexes);

db.bad.drop();
t.drop();
