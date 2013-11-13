// Test the 'create' with primary key specifications

t = db.pk_create;
t.drop();

assert.commandWorked(db.runCommand({ create: 'pk_create', primaryKey: { _id: 1 } }));
t.drop();
assert.commandFailed(db.runCommand({ create: 'pk_create', primaryKey: { _id: -1 } }));
t.drop();
assert.commandFailed(db.runCommand({ create: 'pk_create', primaryKey: { _id: 'hashed' } }));
t.drop();
assert.commandWorked(db.runCommand({ create: 'pk_create', primaryKey: { a: 1, _id: 1 } }));
t.drop();
assert.commandFailed(db.runCommand({ create: 'pk_create', primaryKey: { a: 1, _id: -1 } }));
t.drop();
assert.commandFailed(db.runCommand({ create: 'pk_create', primaryKey: { a: 1, _id: 'hashed' } }));
t.drop();
assert.commandWorked(db.runCommand({ create: 'pk_create', primaryKey: { a: 1, b: 1, _id: 1 } }));
t.drop();
assert.commandFailed(db.runCommand({ create: 'pk_create', primaryKey: { a: 1, b: 1, _id: 1, z: 1 } }));
t.drop();
assert.commandFailed(db.runCommand({ create: 'pk_create', primaryKey: { a: 1 } }));
t.drop();
assert.commandFailed(db.runCommand({ create: 'pk_create', primaryKey: { _id: 1 }, sparse: true }));
t.drop();
assert.commandFailed(db.runCommand({ create: 'pk_create', primaryKey: { a: 1, _id: 1 }, sparse: true }));
t.drop();
assert.commandFailed(db.runCommand({ create: 'pk_create', primaryKey: { _id: 1, a: 1, _id: 1 } }));
t.drop();
assert.commandWorked(db.runCommand({ create: 'pk_create', primaryKey: { a: 1, _id: 'hashed', _id: 1 } }));
t.drop();
assert.commandFailed(db.runCommand({ create: 'pk_create', primaryKey: { _id: 'hashed', a: 'hashed', _id: 1 } }));
t.drop();
assert.commandWorked(db.runCommand({ create: 'pk_create', primaryKey: { _id: -1, _id: 1 } }));
t.drop();
assert.commandFailed(db.runCommand({ create: 'pk_create', primaryKey: { _id: 1, _id: -1 } }));
t.drop();
