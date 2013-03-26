add_library(bson STATIC
  mutable/document
  mutable/element
  util/bson_extract
  ../util/safe_num
  bson_validate
  oid
  ../db/jsobj
  ../db/json
  )
add_dependencies(bson generate_error_codes generate_action_types)
target_link_libraries(bson LINK_PUBLIC
  base
  md5
  stringutils
  platform
  )

install(FILES bsondemo/bsondemo.cpp
  DESTINATION src/mongo/bson/bsondemo
  COMPONENT tokumx_client_sources
  )

add_library(mutable_bson_test_utils STATIC
  mutable/mutable_bson_test_utils
  )
add_dependencies(mutable_bson_test_utils generate_error_codes generate_action_types)
target_link_libraries(mutable_bson_test_utils LINK_PUBLIC
  bson
  )