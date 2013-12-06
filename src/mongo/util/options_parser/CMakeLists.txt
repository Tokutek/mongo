add_library(options_parser STATIC
  environment
  value
  constraints
  option_section
  option_description
  options_parser
  startup_option_init
  startup_options
  )
add_dependencies(options_parser generate_error_codes generate_action_types)
target_link_libraries(options_parser LINK_PUBLIC
  bson
  ${YAML_LIBRARIES}
  ${PCRE_LIBRARIES}
  )
