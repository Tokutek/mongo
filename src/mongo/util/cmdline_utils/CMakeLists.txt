add_library(cmdline_utils STATIC
  censor_cmdline
  )
add_dependencies(cmdline_utils generate_error_codes generate_action_types)
target_link_libraries(cmdline_utils LINK_PUBLIC
  bson
  )

if (BUILD_TESTING)
  foreach (test
      censor_cmdline_test
      )
    add_executable(${test} ${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    link_recursive_deps(${test}
      COMBINED_LIBNAME cmdline_utils_unittest_deps
      cmdline_utils
      unittest_main
      unittest_crutch
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()
endif ()