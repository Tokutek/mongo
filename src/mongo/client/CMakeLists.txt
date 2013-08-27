set(common_files
  auth_helpers
  connpool
  dbclient
  dbclient_rs
  dbclientcursor
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
  target_link_libraries(sasl_client_session LINK_PUBLIC
    foundation
    ${SASL_LIBRARIES}
    )
endif ()
add_library(mongocommon_client STATIC ${common_files})
add_dependencies(mongocommon_client generate_error_codes generate_action_types)
target_link_libraries(mongocommon_client LINK_PUBLIC
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
  target_link_libraries(mongocommon_client LINK_PUBLIC
    sasl_client_session
    )
endif ()

add_library(client_parallel STATIC
  parallel
  )
add_dependencies(client_parallel generate_error_codes generate_action_types install_tdb_h)

add_library(gridfs STATIC
  gridfs
  )
add_dependencies(gridfs generate_error_codes generate_action_types)

add_library(clientandshell STATIC
  clientAndShell
  )
add_dependencies(clientandshell generate_error_codes generate_action_types)
target_link_libraries(clientandshell LINK_PUBLIC
  mongocommon
  defaultversion
  gridfs
  )

add_library(allclient STATIC
  clientOnly
  )
add_dependencies(allclient generate_error_codes generate_action_types)
target_link_libraries(allclient LINK_PUBLIC
  clientandshell
  )

add_subdirectory(examples)
