// ensure that we can insert > 16MB of documents in a single transaction

db.test.drop();

// create an 8MB string
a='a';
for (var i=0; i<23; i++) { a += a; }

db.runCommand("beginTransaction")
db.test.insert({a:a, x:1});
db.test.insert({a:a, x:2});
db.test.insert({a:a, x:3});
db.runCommand("commitTransaction")

assert.eq(3,db.test.count());
db.test.find().forEach(function (x) { assert.eq(x.a.length,a.length); print(x._id,x.a.length); } );

