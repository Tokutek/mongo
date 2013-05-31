// Test that db.save() correctly performs secondary key unique checks
t = db.save_unique;

t.drop();
t.ensureIndex({ a: 1 }, { unique: true });
t.save({_id: 1, a: 1 });
assert.eq(1, t.count({ a: 1}));
t.save({_id: 2, a: 1 });
assert(db.getLastError(), "uniqueness violation uncaught (non-clustering) ");

t.drop();
t.ensureIndex({ a: 1 }, { unique: true, clustering: true });
t.save({_id: 1, a: 1 });
assert.eq(1, t.count({ a: 1}));
t.save({_id: 2, a: 1 });
assert(db.getLastError(), "uniqueness violation uncaught (clustering) ");
