add_library(framework_options STATIC
  framework_options
  )
add_dependencies(framework_options generate_error_codes generate_action_types)
target_link_libraries(framework_options LINK_PUBLIC
  options_parser
  )

add_library(testframework STATIC
  framework
  framework_options_init
  )
add_dependencies(testframework generate_error_codes generate_action_types)
target_link_libraries(testframework LINK_PUBLIC
  unittest
  options_parser
  framework_options
  )

add_library(mocklib STATIC
  mock/mock_conn_registry
  mock/mock_dbclient_connection
  mock/mock_dbclient_cursor
  mock/mock_remote_db_server
  mock/mock_replica_set
  )
add_dependencies(mocklib generate_error_codes generate_action_types)
target_link_libraries(mocklib LINK_PUBLIC
  mongocommon
  )

file(GLOB testfiles *.cpp)
foreach (framework_filename
    framework.cpp
    framework_options.cpp
    framework_options_init.cpp
    framework_options_test.cpp
    )
  set(_framework_fullpath)
  foreach (testfile ${testfiles})
    if (testfile MATCHES "${framework_filename}$")
      set(_framework_fullpath "${testfile}")
    endif ()
  endforeach ()
  list(REMOVE_ITEM testfiles ${_framework_fullpath})
endforeach ()
add_executable(smokeTest ${testfiles})
add_dependencies(smokeTest generate_error_codes generate_action_types install_tdb_h)
if (NOT APPLE)
  target_link_whole_libraries(smokeTest
    jemalloc
    )
endif ()
link_recursive_deps(smokeTest
  mongocommon
  serveronly
  coreserver
  coredb
  testframework
  gridfs
  s_upgrade
  mocklib
  authmocks
  ${TokuKV_LIBRARIES}
  ${TOKUMX_SSL_LIBRARIES}
  )
