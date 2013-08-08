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
target_link_libraries(base LINK_PUBLIC
  murmurhash3
  )

install(FILES generate_error_codes.py error_codes.err
  DESTINATION src/mongo/base
  COMPONENT tokumx_client_sources
  )
