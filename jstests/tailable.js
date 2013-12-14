// simple test for tailable cursors

t = db.capped;
t2 = db.capped1;
t.drop();
t2.drop();
db.runCommand({ create: 'capped', capped: true, size: 1024 });
t.insert({ _id: 0 });
t.insert({ _id: 1 });
// start a parallel shell to count the number of unique _id values we read from the tailable cursor.
s = startParallelShell('var ok = false; ' +
                       'db.capped.find().addOption(34).forEach(function(o) { ' +
                       '    print("tailable cursor got " +  tojson(o)); ' +
                       '    db.capped1.update({ _id: o._id }, { $inc: { seen: 1 } }, { multi: false, upsert: true }); ' +
                       '});' +
                       'assert.eq(null, db.getLastError())')
print("sleeping 2 seconds, hoping bgthread begins soon...");
sleep(2000);
print("inserting 2, sleeping 1 sec");
t.insert({ _id: 2 });
sleep(1000);
print("inserting 3, sleeping 500 ms");
t.insert({ _id: 3 });
sleep(500);
print("inserting 4,5 together (will succeed), sleeping 1 sec");
t.insert([{ _id: 4 }, { _id: 5 }]); // these commits together
sleep(1000);
print("inserting 6,6 together (will fail), sleeping 500 ms");
t.insert([{ _id: 6 }, { _id: 6 }]); // these roll back!
sleep(500);
print("inserting 6, sleeping 250 ms");
t.insert({ _id: 6 });
sleep(250);
print("ok");

// t == t2
// set seen: 1 for all of t first, since that's what should be in t2
var expected = new Array();
for (i = 0; i < 7; i++) {
    expected[i] = { _id: i, seen: 1 };
}
assert.eq(expected, t2.find().toArray());
// t2 did not double-read from t
t2.find().forEach(function(o) { assert.eq(1, o.seen, tojson(o)); });
