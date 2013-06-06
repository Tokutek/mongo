// @file dur_big_update.js
//
// this tests writing 1GB in an update to make sure we commit periodically

var path = "/data/db/dur_big_update";

conn = startMongodEmpty("--port", 30001, "--dbpath", path, "--dur", "--durOptions", 8);
d = conn.getDB("test");
d.foo.drop();

for (var i=0; i<1024; i++){
    d.foo.insert({_id:i});
}

big_string = 'xxxxxxxxxxxxxxxx';
while (big_string.length < 1024*1024) {
    big_string += big_string;
}

d.foo.update({}, {$set: {big_string: big_string}}, false, /*multi*/true);
err = d.getLastErrorObj();

assert(err.err == null);
assert(err.n == 1024);

d.dropDatabase();

for (var i=0; i<1024; i++){
    d.foo.insert({_id:i});
}

// Do it again but in a db.eval
d.eval(
    function(big_string) {
        new Mongo().getDB("test").foo.update({}, {$set: {big_string: big_string}}, false, /*multi*/true)
    }, big_string); // Can't pass in connection or DB objects

err = d.getLastErrorObj();

assert(err.err == null);
assert(err.n == 1024);

// free up space
d.dropDatabase();

stopMongod(30001);

print("dur big tomic update SUCCESS");
