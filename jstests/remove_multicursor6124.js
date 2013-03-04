t = db.remove_multicursor6124;
t.drop()

// Simple case, no multikey indexes.
t.ensureIndex({a:1});
t.ensureIndex({b:1});
t.insert({_id:1, a:1, b:1})
t.insert({_id:2, a:1, b:1})
t.insert({_id:3, a:1, b:0})
t.remove({a:1, b:1}, false)

t.drop()

// Multikey case
t.ensureIndex({a:1})
t.ensureIndex({b:1})
t.insert({_id:1, a:1, b:[1,2,3]})
t.insert({_id:2, a:1, b:2})
t.insert({_id:3, a:1, b:[2,3]})
t.remove({a:1, b:1}, false);
