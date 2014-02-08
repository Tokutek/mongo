add_library(testframework STATIC
  framework
  )
add_dependencies(testframework generate_error_codes generate_action_types)
target_link_libraries(testframework LINK_PRIVATE
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
target_link_libraries(mocklib LINK_PRIVATE
  mongocommon
  )

file(GLOB testfiles *.cpp)
list(REMOVE_ITEM testfiles framework.cpp)
add_executable(smokeTest ${testfiles})
add_dependencies(smokeTest generate_error_codes generate_action_types install_tdb_h)
if (NOT APPLE)
  target_link_whole_libraries(smokeTest
    jemalloc
    )
endif ()
target_link_whole_libraries(smokeTest
  mongocommon
  serveronly
  authservercommon
  coreserver
  coredb
  )
target_link_libraries(smokeTest
  testframework
  )
target_link_whole_libraries(smokeTest
  gridfs
  notmongodormongos
  s_upgrade
  mocklib
  base
  )
target_link_libraries(smokeTest
  ${TokuKV_LIBRARIES}
  ${TOKUMX_SSL_LIBRARIES}
  )
