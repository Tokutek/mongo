if (BUILD_TESTING)
  set(js_ctest_root "${CMAKE_BINARY_DIR}/ctest/mongo/js")
  set(js_dbpath "${js_ctest_root}/db")
  set(js_logpath "${js_ctest_root}/log")
  file(MAKE_DIRECTORY "${js_dbpath}")
  add_test(
    NAME mongo/js/start-mongod
    COMMAND $<TARGET_FILE:mongod>
      --port 27999
      --dbpath "${js_dbpath}"
      --fork
      --logpath "${js_logpath}"
      --setParameter enableTestCommands=1
    )
  add_test(
    NAME mongo/js/check-mongod
    COMMAND /bin/bash -c "for i in `seq 1 60`; do if $<TARGET_FILE:mongo> --port 27999 --eval 'db.version()'; then break; fi; if [ $i = 60 ]; then exit 1; fi; done"
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
        DEPENDS mongo/js/check-mongod
        )
      set_property(TEST mongo/js/stop-mongod APPEND PROPERTY
        DEPENDS mongo/js/${jstest}
        )
    endif ()
  endforeach ()
endif ()