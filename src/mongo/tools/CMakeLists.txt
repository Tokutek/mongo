add_library(alltools STATIC
  tool
  stat_util
  tool_logger
  )
add_dependencies(alltools generate_error_codes generate_action_types)
target_link_libraries(alltools LINK_PUBLIC
  serveronly
  coreserver
  coredb
  options_parser
  authmocks
  )

add_library(tool_options STATIC
  tool_options
  )
add_dependencies(tool_options generate_error_codes generate_action_types)
target_link_libraries(tool_options LINK_PUBLIC
  server_options
  options_parser
  serveronly
  coreserver
  coredb
  )
