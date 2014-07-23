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
  auth_external_state
  authorization_manager
  role_graph
  role_name
  authorization_session
  principal
  principal_name
  principal_set
  privilege
  privilege_set
  )
add_dependencies(authcore generate_error_codes generate_action_types)
target_link_libraries(authcore LINK_PUBLIC
  base
  bson
  stringutils
  )

add_library(authservercommon STATIC
  auth_external_state_server_common
  auth_server_parameters
  security_key
  )
add_dependencies(authservercommon generate_error_codes generate_action_types)
target_link_libraries(authservercommon LINK_PUBLIC
  authcore
  )

add_library(authmongod STATIC
  auth_external_state_d
  )
add_dependencies(authmongod generate_error_codes generate_action_types)
target_link_libraries(authmongod LINK_PUBLIC
  authservercommon
  )

add_library(authmongos STATIC
  auth_external_state_s
  )
add_dependencies(authmongos generate_error_codes generate_action_types)
target_link_libraries(authmongos LINK_PUBLIC
  authservercommon
  )

if (BUILD_TESTING)
  foreach (test
      action_set_test
      principal_set_test
      privilege_set_test
      role_graph_test
      authorization_session_test
      )
    add_executable(${test} ${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    link_recursive_deps(${test}
      COMBINED_LIBNAME auth_unittest_deps
      authcore
      unittest_main
      unittest_crutch
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()
endif ()

install(FILES generate_action_types.py action_types.txt
  DESTINATION src/mongo/db/auth
  COMPONENT tokumx_client_sources
  )
