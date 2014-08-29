add_library(cmdline_utils STATIC
  censor_cmdline
  )
add_dependencies(cmdline_utils generate_error_codes generate_action_types)
target_link_libraries(cmdline_utils LINK_PUBLIC
  bson
  )
