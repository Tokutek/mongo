add_library(testframework STATIC
  framework
  )
add_dependencies(testframework generate_error_codes generate_action_types)
target_link_libraries(testframework LINK_PUBLIC
  unittest
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
foreach (testfile ${testfiles})
  if (testfile MATCHES "framework.cpp$")
    set(framework_fullpath "${testfile}")
  endif ()
endforeach ()
list(REMOVE_ITEM testfiles ${framework_fullpath})
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
  ${TokuKV_LIBRARIES}
  ${TOKUMX_SSL_LIBRARIES}
  )
