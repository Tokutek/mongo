var d = db.getSiblingDB("getNamespaces_after_close");
d.dropDatabase();

d.createCollection("foo");
d.createCollection("bar");

// the db stats command uses NamespaceIndex::getNamespaces() to find collections to iterate over.
assert.eq(4, d.stats().collections, "before");

// rolling back a rename leaves a collection closed (not in the NamespaceIndex's cache)
d.runCommand("beginTransaction");
d.adminCommand({renameCollection: d + ".bar", to: d + ".baz"});
assert.eq(4, d.stats().collections, "during");
d.runCommand("rollbackTransaction");

// ensure that we open the collection back up and get the right answer
assert.eq(4, d.stats().collections, "after");
assert.eq(4, d.stats().collections, "again");
