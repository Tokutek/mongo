add_library(expressions STATIC
  expression
  expression_array
  expression_internal
  expression_leaf
  expression_tree
  expression_parser
  expression_parser_tree
  )
add_dependencies(expressions generate_error_codes generate_action_types)
target_link_libraries(expressions LINK_PUBLIC
  bson
  db_common
  ${PCRE_LIBRARIES}
  )