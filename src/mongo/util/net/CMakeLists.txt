add_library(network STATIC
  sock
  socket_poll
  ssl_manager
  ssl_options
  httpclient
  message
  message_port
  listen
  )
add_dependencies(network generate_error_codes generate_action_types)
target_link_libraries(network LINK_PUBLIC
  options_parser
  background_job
  fail_point
  foundation
  server_options_core
  )