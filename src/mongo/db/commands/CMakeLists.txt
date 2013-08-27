add_library(usercommandsparser STATIC
  user_management_commands_parser
  )
add_dependencies(usercommandsparser generate_error_codes generate_action_types)
target_link_libraries(usercommandsparser LINK_PUBLIC
  authcore
  bson
  )
