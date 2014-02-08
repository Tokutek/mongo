add_library(stacktrace STATIC
  stacktrace
  )
add_dependencies(stacktrace generate_error_codes generate_action_types)

add_library(foundation STATIC
  assert_util
  concurrency/mutexdebugger
  debug_util
  exception_filter_win32
  file
  log
  platform_init
  signal_handlers
  text
  time_support
  timer
  )
add_dependencies(foundation generate_error_codes generate_action_types)
target_link_libraries(foundation LINK_PRIVATE
  stacktrace
  base
  platform
  ${Boost_LIBRARIES}
  )

add_library(mongocommon_util STATIC
  background
  intrusive_counter
  util
  trace
  ramlog
  progress_meter
  concurrency/task
  concurrency/thread_pool
  password
  concurrency/rwlockimpl
  histogram
  concurrency/spin_lock
  text_startuptest
  stack_introspect
  concurrency/synchronization
  net/sock
  net/ssl_manager
  net/httpclient
  net/message
  net/message_port
  net/listen
  startup_test
  version
  )
add_dependencies(mongocommon_util generate_error_codes generate_action_types)
target_link_libraries(mongocommon_util LINK_PRIVATE
  bson
  foundation
  mongohasher
  md5
  processinfo
  stacktrace
  stringutils
  fail_point
  ${PCRE_LIBRARIES}
  murmurhash3
  ${Boost_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT}
  )

add_library(stringutils STATIC
  stringutils
  base64
  )
add_dependencies(stringutils generate_error_codes generate_action_types)

add_library(md5 STATIC
  md5
  )
add_dependencies(md5 generate_error_codes generate_action_types)

add_library(processinfo STATIC
  processinfo
  processinfo_${system_suffix}
  )
add_dependencies(processinfo generate_error_codes generate_action_types)
target_link_libraries(processinfo LINK_PRIVATE
  foundation
  bson
  )

if (system_suffix STREQUAL darwin OR
    system_suffix STREQUAL freebsd OR
    system_suffix STREQUAL sunos5)
  # only linux2 and win32 exist right now
  add_library(systeminfo STATIC
    systeminfo
    systeminfo_none
    )
else ()
  add_library(systeminfo STATIC
    systeminfo
    systeminfo_${system_suffix}
    )
endif ()
add_dependencies(systeminfo generate_error_codes generate_action_types)

add_library(fail_point STATIC
  fail_point
  fail_point_registry
  fail_point_service
  )
add_dependencies(fail_point generate_error_codes generate_action_types install_tdb_h)
target_link_libraries(fail_point LINK_PRIVATE
  foundation
  bson
  )

# WONTFIX: ntservice, ntservice_test

if (BUILD_TESTING)
  foreach (test_source
      concurrency/partitioned_counter_test
      descriptive_stats_test
      fail_point_test
      processinfo_test
      safe_num_test
      string_map_test
      stringutils_test
      text_test
      )
    string(REGEX REPLACE "^.*/" "" test ${test_source})
    add_executable(${test} ${test_source})
    add_dependencies(${test} generate_error_codes generate_action_types)
    target_link_whole_libraries(${test}
      unittest_main
      foundation
      unittest_crutch
      base
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()

  target_link_libraries(descriptive_stats_test bson)
  target_link_libraries(fail_point_test fail_point)
  target_link_libraries(processinfo_test processinfo)
  target_link_libraries(safe_num_test bson)
  target_link_libraries(string_map_test bson)
  target_link_libraries(stringutils_test stringutils)

  # no crutch
  add_executable(sock_test net/sock_test)
  add_dependencies(sock_test generate_error_codes generate_action_types)
  target_link_libraries(sock_test
    unittest_main
    foundation
    )
  target_link_whole_libraries(sock_test
    mongocommon
    base
    ${TOKUMX_SSL_LIBRARIES}
    )

  add_executable(md5_test md5_test md5main)
  add_dependencies(md5_test generate_error_codes generate_action_types)
  target_link_whole_libraries(md5_test
    md5
    unittest_main
    foundation
    unittest_crutch
    base
    ${TOKUMX_SSL_LIBRARIES}
    )
endif ()
