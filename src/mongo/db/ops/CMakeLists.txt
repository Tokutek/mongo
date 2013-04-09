add_library(update STATIC
  path_support
  )
add_dependencies(update generate_error_codes generate_action_types)
target_link_libraries(update LINK_PUBLIC
  bson
  foundation
  db_common
  )
