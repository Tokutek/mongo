add_library(geometry STATIC
  hash
  shapes
  )
add_dependencies(geometry generate_error_codes generate_action_types)
target_link_libraries(geometry LINK_PUBLIC
  bson
  )

add_library(geoparser STATIC
  geoparser
  )
add_dependencies(geoparser generate_error_codes generate_action_types)
target_link_libraries(geoparser LINK_PUBLIC
  bson
  geometry
  s2
  )

add_library(geoquery STATIC
  geoquery
  )
add_dependencies(geoquery generate_error_codes generate_action_types)
target_link_libraries(geoquery LINK_PUBLIC
  bson
  geometry
  s2
  )
