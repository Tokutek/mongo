// Test that loader commands can be used by normal authed users, not just AnyDatabase roles
var conn = MongoRunner.runMongod({auth : ''});

var adminDB = conn.getDB('admin');
assert.commandWorked(adminDB.dropDatabase());
adminDB.addUser({user:'admin',
                 pwd:'password',
                 roles:['userAdminAnyDatabase', 'dbAdminAnyDatabase']});

adminDB.auth('admin', 'password');

function checkLoaderCommands(loginDB, testDB) {
    testDB.coll.drop();
    loginDB.logout();
    assert.commandFailed(testDB.beginTransaction());
    // welp, this won't work anyway because we're not in a MST
    assert.commandFailed(testDB.runCommand({beginLoad: 1, ns: testDB.getName() + '.coll', indexes: [], options: {}}));
    loginDB.auth('spencer', 'password');
    assert.commandWorked(testDB.beginTransaction());
    assert.commandWorked(testDB.runCommand({beginLoad: 1, ns: testDB.getName() + '.coll', indexes: [], options: {}}));
    assert.commandWorked(testDB.runCommand('abortLoad'));
    assert.commandWorked(testDB.rollbackTransaction());
    loginDB.logout();
}

{
    var authedTestDB = adminDB.getSiblingDB('txn_commands_compat_rw');
    authedTestDB.system.users.remove();
    assert.eq(null, authedTestDB.getLastError());
    authedTestDB.addUser('spencer', 'password');
    assert.eq(null, authedTestDB.getLastError());

    var testDB = conn.getDB('txn_commands_compat_rw');
    checkLoaderCommands(testDB, testDB);
}

var goodRoles = [
    ['readWrite'],
];

for (var i = 0; i < goodRoles.length; ++i) {
    adminDB.auth('admin', 'password');

    var authedTestDB = adminDB.getSiblingDB('txn_commands_' + goodRoles[i][0]);
    authedTestDB.system.users.remove();
    assert.eq(null, authedTestDB.getLastError());
    authedTestDB.addUser({user:'spencer',
                          pwd:'password',
                          roles:goodRoles[i]});
    assert.eq(null, authedTestDB.getLastError());

    var testDB = conn.getDB('txn_commands_' + goodRoles[i][0])
    checkLoaderCommands(testDB, testDB);
}

goodRoles = [
    ['readWriteAnyDatabase'],
];

for (var i = 0; i < goodRoles.length; ++i) {
    adminDB.auth('admin', 'password');

    adminDB.system.users.remove({user: 'spencer'});
    assert.eq(null, adminDB.getLastError());
    adminDB.addUser({user:'spencer',
                     pwd:'password',
                     roles:goodRoles[i]});
    assert.eq(null, adminDB.getLastError());

    var testDB = conn.getDB('txn_commands_' + goodRoles[i][0]);
    checkLoaderCommands(adminDB, testDB);
}
