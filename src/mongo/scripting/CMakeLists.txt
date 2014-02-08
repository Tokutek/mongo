add_library(bson_template_evaluator STATIC
  bson_template_evaluator
  )
add_dependencies(bson_template_evaluator generate_error_codes generate_action_types)
target_link_libraries(bson_template_evaluator LINK_PRIVATE bson)

if (JS_ENGINE STREQUAL v8)
  include_directories(${V8_INCLUDE_DIRS})
  add_library(scripting STATIC
    engine
    utils
    bench
    engine_v8
    v8_db
    v8_utils
    v8_profiler
    )
  add_dependencies(scripting generate_error_codes generate_action_types)
  target_link_libraries(scripting LINK_PRIVATE
    bson_template_evaluator
    ${V8_LIBRARIES}
    mongocommon
    stringutils
    )
elseif (JS_ENGINE STREQUAL sm)
  message(SEND_ERROR "spidermonkey not supported with cmake")
else ()
  message(SEND_ERROR "unknown js engine")
endif ()

if (BUILD_TESTING)
  foreach (test
      bson_template_evaluator_test
      v8_deadline_monitor_test
      )
    add_executable(${test} ${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    target_link_libraries(${test}
      unittest_main
      foundation
      unittest_crutch
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()
  
  target_link_libraries(bson_template_evaluator_test bson_template_evaluator)
endif ()
