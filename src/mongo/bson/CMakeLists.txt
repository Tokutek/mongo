add_library(bson STATIC
  mutable/mutable_bson
  mutable/mutable_bson_builder
  mutable/mutable_bson_heap
  mutable/mutable_bson_internal
  util/bson_extract
  ../util/safe_num
  bson_validate
  oid
  ../db/jsobj
  ../db/json
  )
add_dependencies(bson generate_error_codes generate_action_types)
target_link_libraries(bson LINK_PRIVATE
  base
  md5
  stringutils
  platform
  )

if (BUILD_TESTING)
  foreach (test_source
      util/builder_test
      util/bson_extract_test
      mutable/mutable_bson_test
      mutable/mutable_bson_heap_test
      mutable/mutable_bson_builder_test
      mutable/mutable_bson_algo_test
      bson_field_test
      bson_validate_test
      )
    string(REGEX REPLACE "^.*/" "" test ${test_source})
    add_executable(${test} ${test_source})
    add_dependencies(${test} generate_error_codes generate_action_types)
    target_link_whole_libraries(${test}
      bson
      unittest_main
      foundation
      unittest_crutch
      base
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()
endif ()

install(FILES bsondemo/bsondemo.cpp
  DESTINATION src/mongo/bson/bsondemo
  COMPONENT tokumx_client_sources
  )
