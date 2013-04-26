function begin() {
    db.runCommand({ 'beginTransaction': 1 });
    assert(!db.getLastError());
}
function commit() {
    db.runCommand({ 'commitTransaction': 1 });
    assert(!db.getLastError());
}
function rollback() {
    db.runCommand({ 'rollbackTransaction': 1 });
    assert(!db.getLastError());
}

db.dropDatabase();
db.TT.insert({});
db.UU.insert({});
db.ZZ.insert({});
shell = startParallelShell( "sleep(1); " +
                            "for (i = 0; i < 2000; i++) { " +
                            "   db.TT.findOne({}); db.UU.findOne({}); db.ZZ.findOne({}); " +
                            "}" )
// Do the fileops abort dance while another shell exercises
// the system with queries/inserts/fileops. This should test
// the threadsafey of the nsindex data structure under various
// locking scenarios.
db.T.insert({});
db.U.insert({});
for (i = 0; i < 1000; i++) {
    begin();
    if (i % 5 == 0) {
        db.T.renameCollection('T2');
    } else if (i % 5 == 1) {
        db.T.drop();
    } else if (i % 5 == 2) {
        db.U.renameCollection('U2');
    } else if (i % 5 == 3) {
        db.U.drop();
    } else if (i % 5 == 4) {
        db.T.drop();
        db.U.drop();
    }
    rollback();
    i++;
}

