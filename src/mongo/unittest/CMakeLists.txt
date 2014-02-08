add_library(unittest STATIC
  unittest
  )
add_dependencies(unittest generate_error_codes generate_action_types)
target_link_libraries(unittest LINK_PRIVATE
  foundation
  )

add_library(unittest_main STATIC
  unittest_main
  )
add_dependencies(unittest_main generate_error_codes generate_action_types)
target_link_libraries(unittest_main LINK_PRIVATE
  unittest
  base
  )

add_library(unittest_crutch STATIC
  crutch
  )
add_dependencies(unittest_crutch generate_error_codes generate_action_types)

if (BUILD_TESTING)
  foreach (test
      unittest_test
      fixture_test
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
