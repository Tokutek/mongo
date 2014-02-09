if (BUILD_TESTING)
  set(js_ctest_root "${CMAKE_BINARY_DIR}/ctest/mongo/js")
  set(js_dbpath "${js_ctest_root}/db")
  set(js_logpath "${js_ctest_root}/log")
  file(MAKE_DIRECTORY "${js_dbpath}")
  add_test(
    NAME mongo/js/clean-mongod-dbpath
    COMMAND bash -c "ls ${js_dbpath}/* | xargs rm -rf"
    )
  add_test(
    NAME mongo/js/start-mongod
    COMMAND $<TARGET_FILE:mongod>
      --port 27999
      --dbpath "${js_dbpath}"
      --fork
      --logpath "${js_logpath}"
      --setParameter enableTestCommands=1
    )
  set_tests_properties(mongo/js/start-mongod PROPERTIES
    DEPENDS mongo/js/clean-mongod-dbpath
    )
  add_test(
    NAME mongo/js/check-mongod
    COMMAND /bin/bash -c "for i in `seq 1 180`; do if $<TARGET_FILE:mongo> --port 27999 --eval 'db.version()'; then break; fi; if [ $i = 180 ]; then exit 1; fi; sleep 1; done"
    )
  set_tests_properties(mongo/js/check-mongod PROPERTIES
    DEPENDS mongo/js/start-mongod
    )
  add_test(
    NAME mongo/js/stop-mongod
    COMMAND $<TARGET_FILE:mongo>
      --port 27999
      --eval "db.adminCommand({shutdown: 1, force: true})"
    )
  set_tests_properties(mongo/js/stop-mongod PROPERTIES
    WILL_FAIL TRUE
    DEPENDS mongo/js/check-mongod
    )

  set(js_auth_ctest_root "${CMAKE_BINARY_DIR}/ctest/mongo/js-auth")
  set(js_auth_dbpath "${js_auth_ctest_root}/db")
  set(js_auth_logpath "${js_auth_ctest_root}/log")
  file(MAKE_DIRECTORY "${js_auth_dbpath}")
  add_test(
    NAME mongo/js-auth/clean-mongod-dbpath
    COMMAND bash -c "ls ${js_auth_dbpath}/* | xargs rm -rf"
    )
  set_tests_properties(mongo/js-auth/clean-mongod-dbpath PROPERTIES
    DEPENDS mongo/js/stop-mongod
    )
  add_test(
    NAME mongo/js-auth/start-mongod
    COMMAND $<TARGET_FILE:mongod>
      --port 27999
      --dbpath "${js_auth_dbpath}"
      --fork
      --logpath "${js_auth_logpath}"
      --setParameter enableTestCommands=1
      --auth
    )
  set_tests_properties(mongo/js-auth/start-mongod PROPERTIES
    DEPENDS mongo/js-auth/clean-mongod-dbpath
    )
  add_test(
    NAME mongo/js-auth/check-mongod
    COMMAND /bin/bash -c "for i in `seq 1 180`; do if $<TARGET_FILE:mongo> --port 27999 --eval 'db.version()'; then break; fi; if [ $i = 180 ]; then exit 1; fi; sleep 1; done"
    )
  set_tests_properties(mongo/js-auth/check-mongod PROPERTIES
    DEPENDS mongo/js-auth/start-mongod
    )
  add_test(
    NAME mongo/js-auth/add-admin-user
    COMMAND $<TARGET_FILE:mongo>
      admin
      --port 27999
      --eval "
db.addUser('admin', 'password');
assert.soon(function() {
    var ret = db.auth('admin', 'password');
    if (ret) {
        if (db.system.users.find({user: 'admin'}).count() == 1) {
            return true;
        }
    }
    db.addUser('admin', 'password');
    return false;
});
"
    )
  set_tests_properties(mongo/js-auth/add-admin-user PROPERTIES
    DEPENDS mongo/js-auth/check-mongod
    )
  add_test(
    NAME mongo/js-auth/stop-mongod
    COMMAND $<TARGET_FILE:mongo>
      admin
      --port 27999
      --eval "db.auth('admin', 'password'); db.runCommand({shutdown: 1, force: true})"
    )
  set_tests_properties(mongo/js-auth/stop-mongod PROPERTIES
    WILL_FAIL TRUE
    DEPENDS mongo/js-auth/add-admin-user
    )

  file(GLOB all_js_tests RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" *.js)
  foreach (jstest ${all_js_tests})
    string(SUBSTRING "${jstest}" 0 1 first_char)
    if (NOT first_char STREQUAL "_")
      string(REGEX REPLACE "\\.js$" "" jstestname "${jstest}")
      add_test(
        NAME mongo/js/${jstest}
        COMMAND $<TARGET_FILE:mongo>
          ${jstestname}
          --port 27999
          "${CMAKE_CURRENT_SOURCE_DIR}/${jstest}"
          --eval "TestData = new Object();
TestData.testDir = '${CMAKE_CURRENT_SOURCE_DIR}';
TestData.testPath = '${CMAKE_CURRENT_SOURCE_DIR}/${jstest}';
TestData.testFile = '${jstest}';
TestData.testName = '${jstestname}'"
        )
      set_tests_properties(mongo/js/${jstest} PROPERTIES
        DEPENDS mongo/js/check-mongod
        )
      set_property(TEST mongo/js/${jstest} APPEND PROPERTY
        ENVIRONMENT "PATH=${CMAKE_BINARY_DIR}/src/mongo:$ENV{PATH}"
        )
      set_property(TEST mongo/js/stop-mongod APPEND PROPERTY
        DEPENDS mongo/js/${jstest}
        )

      add_test(
        NAME mongo/js-auth/${jstest}
        COMMAND $<TARGET_FILE:mongo>
          ${jstestname}
          --port 27999
          "${CMAKE_CURRENT_SOURCE_DIR}/${jstest}"
          --eval "TestData = new Object();
TestData.testDir = '${CMAKE_CURRENT_SOURCE_DIR}';
TestData.testPath = '${CMAKE_CURRENT_SOURCE_DIR}/${jstest}';
TestData.testFile = '${jstest}';
TestData.testName = '${jstestname}';
TestData.auth = true;
jsTest.authenticate(db.getMongo())"
        )
      set_tests_properties(mongo/js-auth/${jstest} PROPERTIES
        DEPENDS mongo/js-auth/add-admin-user
        )
      set_property(TEST mongo/js-auth/${jstest} APPEND PROPERTY
        ENVIRONMENT "PATH=${CMAKE_BINARY_DIR}/src/mongo:$ENV{PATH}"
        )
      set_property(TEST mongo/js-auth/stop-mongod APPEND PROPERTY
        DEPENDS mongo/js-auth/${jstest}
        )
    endif ()
  endforeach ()

  set_tests_properties(
    mongo/js/queryoptimizer3.js
    mongo/js-auth/queryoptimizer3.js
    PROPERTIES
    TIMEOUT 3000
    )
endif ()