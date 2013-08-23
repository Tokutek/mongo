set_source_files_properties(
  "${CMAKE_CURRENT_BINARY_DIR}/action_type.cpp"
  PROPERTIES GENERATED TRUE)
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/action_type.h"
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/action_type.cpp"
  COMMAND
    "${PYTHON2}"
        "${CMAKE_CURRENT_SOURCE_DIR}/generate_action_types.py"
        "${CMAKE_CURRENT_SOURCE_DIR}/action_types.txt"
        "${CMAKE_CURRENT_BINARY_DIR}/action_type.h"
        "${CMAKE_CURRENT_BINARY_DIR}/action_type.cpp"
  DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/generate_action_types.py"
    "${CMAKE_CURRENT_SOURCE_DIR}/action_types.txt"
  )
add_custom_target(generate_action_types DEPENDS
  "${CMAKE_CURRENT_BINARY_DIR}/action_type.h"
  "${CMAKE_CURRENT_BINARY_DIR}/action_type.cpp"
  )

add_library(serverauth STATIC
  mongo_authentication_session
  )
add_dependencies(serverauth generate_error_codes generate_action_types)

add_library(authcore STATIC
  action_set
  "${CMAKE_CURRENT_BINARY_DIR}/action_type.cpp"
  authorization_manager
  authorization_session
  authz_manager_external_state
  authz_session_external_state
  privilege
  privilege_document_parser
  role_graph
  role_name
  user
  user_name
  user_set
  )
add_dependencies(authcore generate_error_codes generate_action_types)
target_link_libraries(authcore LINK_PUBLIC
  base
  bson
  stringutils
  )

add_library(authservercommon STATIC
  auth_server_parameters
  authorization_manager_global
  authz_session_external_state_server_common
  security_key
  )
add_dependencies(authservercommon generate_error_codes generate_action_types)
target_link_libraries(authservercommon LINK_PUBLIC
  authcore
  )

add_library(authmongod STATIC
  authz_manager_external_state_d
  authz_session_external_state_d
  )
add_dependencies(authmongod generate_error_codes generate_action_types)
target_link_libraries(authmongod LINK_PUBLIC
  authservercommon
  )

add_library(authmongos STATIC
  authz_manager_external_state_s
  authz_session_external_state_s
  )
add_dependencies(authmongos generate_error_codes generate_action_types)
target_link_libraries(authmongos LINK_PUBLIC
  authservercommon
  )

add_library(authmocks STATIC
  authz_manager_external_state_mock
  )
add_dependencies(authmocks generate_error_codes generate_action_types)
target_link_libraries(authmocks
  ## TODO: use update driver in authmocks once it's merged
  # update_driver
  expressions
  )

install(FILES generate_action_types.py action_types.txt
  DESTINATION src/mongo/db/auth
  COMPONENT tokumx_client_sources
  )
