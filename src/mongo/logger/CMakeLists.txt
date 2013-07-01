add_library(logger STATIC
  console
  log_manager
  log_severity
  logger
  logstream_builder
  message_event_utf8_encoder
  message_log_domain
  ramlog
  rotatable_file_manager
  rotatable_file_writer
  )
add_dependencies(logger generate_error_codes generate_action_types install_tdb_h)
target_link_libraries(logger LINK_PUBLIC
  base
  foundation
  thread_name
  )

if (BUILD_TESTING)
  foreach (test
      log_test
      rotatable_file_writer_test
      )
    add_executable(${test} ${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    link_recursive_deps(${test}
      COMBINED_LIBNAME logger_unittest_deps
      logger
      unittest_main
      unittest_crutch
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()
endif ()