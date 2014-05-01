// Test that transactions can be used by normal authed users, not just AnyDatabase roles
var conn = MongoRunner.runMongod({auth : ''});

var adminDB = conn.getDB('admin');
assert.commandWorked(adminDB.dropDatabase());
adminDB.addUser({user:'admin',
                 pwd:'password',
                 roles:['userAdminAnyDatabase', 'dbAdminAnyDatabase']});

adminDB.auth('admin', 'password');

function checkTransactionCommands(loginDB, testDB, serializableOk) {
    loginDB.logout();
    assert.commandFailed(testDB.beginTransaction());
    loginDB.auth('spencer', 'password');
    assert.commandWorked(testDB.beginTransaction());
    assert.commandWorked(testDB.rollbackTransaction());
    var res = testDB.beginTransaction('serializable');
    if (serializableOk) {
        assert.commandWorked(res);
        assert.commandWorked(testDB.rollbackTransaction());
    } else {
        assert.commandFailed(res);
    }
    loginDB.logout();
}

{
    var authedTestDB = adminDB.getSiblingDB('txn_commands_compat_ro');
    authedTestDB.system.users.remove();
    assert.eq(null, authedTestDB.getLastError());
    authedTestDB.addUser('spencer', 'password', true);
    assert.eq(null, authedTestDB.getLastError());

    var testDB = conn.getDB('txn_commands_compat_ro');
    checkTransactionCommands(testDB, testDB, false);
}
{
    var authedTestDB = adminDB.getSiblingDB('txn_commands_compat_rw');
    authedTestDB.system.users.remove();
    assert.eq(null, authedTestDB.getLastError());
    authedTestDB.addUser('spencer', 'password');
    assert.eq(null, authedTestDB.getLastError());

    var testDB = conn.getDB('txn_commands_compat_rw');
    checkTransactionCommands(testDB, testDB, true);
}

var goodRoles = [
    ['read'],
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
    checkTransactionCommands(testDB, testDB, goodRoles[i][0] == 'readWrite');
}

goodRoles = [
    ['readAnyDatabase'],
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
    checkTransactionCommands(adminDB, testDB, goodRoles[i][0] == 'readWriteAnyDatabase');
}
