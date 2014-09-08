add_library(path STATIC
  path
  path_internal
  )
add_dependencies(path generate_error_codes generate_action_types)
target_link_libraries(path LINK_PUBLIC
  bson
  db_common
  )

add_library(expressions STATIC
  expression
  expression_array
  expression_leaf
  expression_tree
  expression_parser
  expression_parser_tree
  matchable
  match_details
  )
add_dependencies(expressions generate_error_codes generate_action_types)
target_link_libraries(expressions LINK_PUBLIC
  bson
  path
  db_common
  ${PCRE_LIBRARIES}
  )

add_library(expressions_geo STATIC
  expression_geo
  expression_parser_geo
  )
add_dependencies(expressions_geo generate_error_codes generate_action_types)
target_link_libraries(expressions_geo LINK_PUBLIC
  expressions
  geoquery
  geoparser
  )

add_library(expressions_where STATIC
  expression_where
  )
add_dependencies(expressions_where generate_error_codes generate_action_types)
target_link_libraries(expressions_where LINK_PUBLIC
  expressions
  )
