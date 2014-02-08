set(common_files
  connpool
  dbclient
  dbclient_rs
  dbclientcursor
  model
  remote_loader
  remote_transaction
  sasl_client_authenticate
  syncclusterconnection
  )
if (USE_SASL_CLIENT)
  list(APPEND common_files sasl_client_authenticate_impl)
  add_library(sasl_client_session STATIC
    sasl_client_session
    )
  add_dependencies(sasl_client_session generate_error_codes generate_action_types)
  target_link_libraries(sasl_client_session LINK_PRIVATE
    foundation
    ${SASL_LIBRARIES}
    )
endif ()
add_library(mongocommon_client STATIC ${common_files})
add_dependencies(mongocommon_client generate_error_codes generate_action_types)
target_link_libraries(mongocommon_client LINK_PRIVATE
  bson
  foundation
  mongohasher
  md5
  processinfo
  stacktrace
  stringutils
  fail_point
  ${PCRE_LIBRARIES}
  murmurhash3
  ${Boost_LIBRARIES}
  )
if (USE_SASL_CLIENT)
  target_link_libraries(mongocommon_client LINK_PRIVATE
    sasl_client_session
    )
endif ()

add_library(client_parallel STATIC
  parallel
  )
add_dependencies(client_parallel generate_error_codes generate_action_types install_tdb_h)

add_library(notmongodormongos STATIC
  connection_factory
  )
add_dependencies(notmongodormongos generate_error_codes generate_action_types)

add_library(gridfs STATIC
  gridfs
  )
add_dependencies(gridfs generate_error_codes generate_action_types)

add_library(clientandshell STATIC
  clientAndShell
  )
add_dependencies(clientandshell generate_error_codes generate_action_types)
target_link_libraries(clientandshell LINK_PRIVATE
  mongocommon
  defaultversion
  gridfs
  notmongodormongos
  )

add_library(allclient STATIC
  clientOnly
  )
add_dependencies(allclient generate_error_codes generate_action_types)
target_link_libraries(allclient LINK_PRIVATE
  clientandshell
  )

add_subdirectory(examples)

if (BUILD_TESTING)
  foreach (test
      dbclient_rs_test
      scoped_db_conn_test
      )
    add_executable(${test} ${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    target_link_libraries(${test}
      unittest_main
      foundation
      )
    target_link_whole_libraries(${test}
      coredb
      coreserver
      coreshard
      dbcmdline
      mongocommon
      mongodandmongos
      mongoscore
      authservercommon
      base
      )
    target_link_libraries(${test}
      ${TokuKV_LIBRARIES}
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()
  target_link_libraries(dbclient_rs_test
    mocklib
    )
endif ()
