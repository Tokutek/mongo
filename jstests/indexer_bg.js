// Test that many concurrent hot index build requests don't crash the server

db.bgidx.drop();
for (i = 0; i < 100; i++) {
    db.bginx.insert({ a: i });
}
js = []
for (i = 0; i < 5; i++) {
    js[i] = ''
    for (k = 0; k < 10; k++) {
        js[i] += 'db.bgidx.ensureIndex({ a' + String(Math.floor(Math.random()*100)) + ' : 1 }, { background: true }); db.getLastError(); print("finished idx build");';
    }
}
s1 = startParallelShell(js[0]);
s2 = startParallelShell(js[1]);
s3 = startParallelShell(js[2]);
s4 = startParallelShell(js[3]);
s5 = startParallelShell(js[3]);
s1(); s2(); s3(); s4(); s5();
