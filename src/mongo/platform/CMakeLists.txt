add_library(platform STATIC
  backtrace
  posix_fadvise
  random
  strcasestr
  )
add_dependencies(platform generate_error_codes generate_action_types)

if (BUILD_TESTING)
  foreach (test
      atomic_word_test
      bits_test
      random_test
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
