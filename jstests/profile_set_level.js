// Check that we can set the profiling level continuosly
// while operations are occurring and nothing breaks.

t = db.profile_get_level;
t.drop();
t.ensureIndex({ a: 1 });
for (i = 0; i < 10 ;i++) {
    t.insert({ a: i });
    assert(!db.getLastError());
}

// Do 50k finds on a bg thread
s = startParallelShell('for (i = 0; i < 20000; i++) { if (i % 2000 == 0) print("bg query " + i); db.profile_get_level.findOne({ a: 5 }); } print("bg thread done"); db.profile_get_level.insert({ a: "done" });');

// Constantly change the profiling level from on to off and see
// if some thing explodes. Keep going until the bg thread is done
// or we've done it way too many times (500 creates/drops is a lot).
for (i = 0; db.profile_get_level.find({ a: 'done' }).hint({ a: 1 }).itcount() == 0 && i < 500; i++) {
    sleep(10);
    if (i % 5 == 0) {
        print("fg setProfilingLevel " + i);
    }
    level = i % 2 == 0 ? 0 : 2;
    db.setProfilingLevel(level);
    if (level == 0) {
        db.system.profile.drop();
    }
}
s();
