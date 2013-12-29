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

# This library contains the initializers to run option parsing.  This is separated into its own
# library because some code that is shared between many different binaries needs to link against the
# options_parser library, but not all these binaries need to actually run the option parsing.
# Linking against this library will cause the option parsing initializer to actually be run.
add_library(options_parser_init STATIC
  options_parser_init
  )
add_dependencies(options_parser_init generate_error_codes generate_action_types)
target_link_libraries(options_parser_init LINK_PUBLIC
  options_parser
  )
