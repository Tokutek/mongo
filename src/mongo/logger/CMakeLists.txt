add_library(logger STATIC
  rotatable_file_writer
  )
add_dependencies(logger generate_error_codes generate_action_types install_tdb_h)
target_link_libraries(logger LINK_PUBLIC
  base
  )

if (BUILD_TESTING)
  add_executable(rotatable_file_writer_test rotatable_file_writer_test)
  add_dependencies(rotatable_file_writer_test generate_error_codes generate_action_types)
  link_recursive_deps(rotatable_file_writer_test
    COMBINED_LIBNAME rotatable_file_writer_test_deps
    logger
    unittest_main
    unittest_crutch
    ${TOKUMX_SSL_LIBRARIES}
    )
endif ()