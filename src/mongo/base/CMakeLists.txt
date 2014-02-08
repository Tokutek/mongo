file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/error_codes.h"
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/error_codes.cpp"
  COMMAND
    "${PYTHON2}"
        "${CMAKE_CURRENT_SOURCE_DIR}/generate_error_codes.py"
        "${CMAKE_CURRENT_SOURCE_DIR}/error_codes.err"
        "${CMAKE_CURRENT_BINARY_DIR}/error_codes.h"
        "${CMAKE_CURRENT_BINARY_DIR}/error_codes.cpp"
  DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/generate_error_codes.py"
    "${CMAKE_CURRENT_SOURCE_DIR}/error_codes.err"
  )
add_custom_target(generate_error_codes DEPENDS
  "${CMAKE_CURRENT_BINARY_DIR}/error_codes.h"
  "${CMAKE_CURRENT_BINARY_DIR}/error_codes.cpp"
  )

add_library(base STATIC
  configuration_variable_manager
  "${CMAKE_CURRENT_BINARY_DIR}/error_codes.cpp"
  global_initializer
  global_initializer_registerer
  init.cpp
  initializer
  initializer_context
  initializer_dependency_graph
  make_string_vector
  parse_number
  status
  string_data
  )
add_dependencies(base generate_error_codes generate_action_types)
target_link_libraries(base LINK_PRIVATE
  murmurhash3
  )

if (BUILD_TESTING)
  foreach (test
      configuration_variable_manager_test
      initializer_dependency_graph_test
      initializer_test
      owned_pointer_vector_test
      owned_pointer_map_test
      parse_number_test
      status_test
      string_data_test
      units_test
      )
    add_executable(${test} ${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    target_link_whole_libraries(${test}
      unittest_main
      foundation
      unittest_crutch
      base
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()
endif ()

install(FILES generate_error_codes.py error_codes.err
  DESTINATION src/mongo/base
  COMPONENT tokumx_client_sources
  )
