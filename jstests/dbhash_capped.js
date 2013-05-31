var t = db.dbhash_capped;
t.drop();

var res = db.runCommand({create: "dbhash_capped", capped: true, size: 1000, autoIndexId: false});
assert.commandWorked(res);
for (var i = 0; i < 10; ++i) {
    t.insert({x:i});
}
assert.eq(null, db.getLastError());

res = db.runCommand("dbhash");
assert.commandWorked(res);
