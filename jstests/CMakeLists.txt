if (BUILD_TESTING)
  set(js_ctest_root "${CMAKE_BINARY_DIR}/ctest/mongo/js")
  set(js_dbpath "${js_ctest_root}/db")
  set(js_logpath "${js_ctest_root}/log")
  file(MAKE_DIRECTORY "${js_dbpath}")
  add_test(
    NAME mongo/server/clean-mongod-dbpath
    COMMAND rm -rf "${js_dbpath}/*"
    )
  add_test(
    NAME mongo/server/start-mongod
    COMMAND $<TARGET_FILE:mongod>
      --port 27999
      --dbpath "${js_dbpath}"
      --fork
      --logpath "${js_logpath}"
      --setParameter enableTestCommands=1
    )
  set_tests_properties(mongo/server/start-mongod PROPERTIES
    DEPENDS mongo/server/clean-mongod-dbpath
    )
  add_test(
    NAME mongo/server/check-mongod
    COMMAND /bin/bash -c "for i in `seq 1 180`; do if $<TARGET_FILE:mongo> --port 27999 --eval 'db.version()'; then break; fi; if [ $i = 180 ]; then exit 1; fi; sleep 1; done"
    )
  set_tests_properties(mongo/server/check-mongod PROPERTIES
    DEPENDS mongo/server/start-mongod
    )
  add_test(
    NAME mongo/server/stop-mongod
    COMMAND $<TARGET_FILE:mongo>
      --port 27999
      --eval "db.adminCommand({shutdown: 1, force: true})"
    )
  set_tests_properties(mongo/server/stop-mongod PROPERTIES
    WILL_FAIL TRUE
    DEPENDS mongo/server/check-mongod
    )

  set(js_ctest_root "${CMAKE_BINARY_DIR}/ctest/mongo/js-auth")
  set(js_dbpath "${js_auth_ctest_root}/db")
  set(js_logpath "${js_auth_ctest_root}/log")
  file(MAKE_DIRECTORY "${js_auth_dbpath}")
  add_test(
    NAME mongo/server-auth/clean-mongod-dbpath
    COMMAND rm -rf "${js_auth_dbpath}/*"
    )
  set_tests_properties(mongo/server-auth/clean-mongod-dbpath PROPERTIES
    DEPENDS mongo/server/stop-mongod
    )
  add_test(
    NAME mongo/server-auth/start-mongod
    COMMAND $<TARGET_FILE:mongod>
      --port 27999
      --dbpath "${js_auth_dbpath}"
      --fork
      --logpath "${js_auth_logpath}"
      --setParameter enableTestCommands=1
      --auth
    )
  set_tests_properties(mongo/server-auth/start-mongod PROPERTIES
    DEPENDS mongo/server-auth/clean-mongod-dbpath
    )
  add_test(
    NAME mongo/server-auth/check-mongod
    COMMAND /bin/bash -c "for i in `seq 1 180`; do if $<TARGET_FILE:mongo> --port 27999 --eval 'db.version()'; then break; fi; if [ $i = 180 ]; then exit 1; fi; sleep 1; done"
    )
  set_tests_properties(mongo/server-auth/check-mongod PROPERTIES
    DEPENDS mongo/server-auth/start-mongod
    )
  add_test(
    NAME mongo/server-auth/add-admin-user
    COMMAND $<TARGET_FILE:mongo>
      admin
      --port 27999
      --eval "if (!db.auth('admin', 'password')) { db.addUser('admin', 'password'); }"
    )
  set_tests_properties(mongo/server-auth/add-admin-user PROPERTIES
    DEPENDS mongo/server-auth/check-mongod
    )
  add_test(
    NAME mongo/server-auth/stop-mongod
    COMMAND $<TARGET_FILE:mongo>
      --port 27999
      --eval "db.adminCommand({shutdown: 1, force: true})"
    )
  set_tests_properties(mongo/server-auth/stop-mongod PROPERTIES
    WILL_FAIL TRUE
    DEPENDS mongo/server-auth/add-admin-user
    )

  file(GLOB all_js_tests RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" *.js)
  foreach (jstest ${all_js_tests})
    string(SUBSTRING "${jstest}" 0 1 first_char)
    if (NOT first_char STREQUAL "_")
      string(REGEX REPLACE "\\.js$" "" jstestname "${jstest}")
      add_test(
        NAME mongo/js/${jstest}
        COMMAND $<TARGET_FILE:mongo>
          --port 27999
          "${CMAKE_CURRENT_SOURCE_DIR}/${jstest}"
          --eval "TestData = new Object();
TestData.testDir = '${CMAKE_CURRENT_SOURCE_DIR}';
TestData.testPath = '${CMAKE_CURRENT_SOURCE_DIR}/${jstest}';
TestData.testFile = '${jstest}';
TestData.testName = '${jstestname}';
var db = db.getSiblingDB('${jstestname}')"
        )
      set_tests_properties(mongo/js/${jstest} PROPERTIES
        DEPENDS mongo/server/check-mongod
        )
      set_property(TEST mongo/server/stop-mongod APPEND PROPERTY
        DEPENDS mongo/js/${jstest}
        )

      add_test(
        NAME mongo/js-auth/${jstest}
        COMMAND $<TARGET_FILE:mongo>
          --port 27999
          "${CMAKE_CURRENT_SOURCE_DIR}/${jstest}"
          --eval "TestData = new Object();
TestData.testDir = '${CMAKE_CURRENT_SOURCE_DIR}';
TestData.testPath = '${CMAKE_CURRENT_SOURCE_DIR}/${jstest}';
TestData.testFile = '${jstest}';
TestData.testName = '${jstestname}';
TestData.auth = true;
jsTest.authenticate(db.getMongo());
var db = db.getSiblingDB('${jstestname}')"
        )
      set_tests_properties(mongo/js-auth/${jstest} PROPERTIES
        DEPENDS mongo/server-auth/add-admin-user
        )
      set_property(TEST mongo/server-auth/stop-mongod APPEND PROPERTY
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