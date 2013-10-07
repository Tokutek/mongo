add_library(options_parser STATIC
  environment
  value
  constraints
  option_section
  options_parser
  startup_option_init
  )
add_dependencies(options_parser generate_error_codes generate_action_types)
target_link_libraries(options_parser LINK_PUBLIC
  bson
  )

if (BUILD_TESTING)
  foreach (test
      options_parser_test
      environment_test
      )
    add_executable(${test} ${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    link_recursive_deps(${test}
      COMBINED_LIBNAME options_parser_unittest_deps
      options_parser
      unittest_main
      unittest_crutch
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()
endif ()