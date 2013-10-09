add_subdirectory(concurrency)
add_subdirectory(options_parser)

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
target_link_libraries(foundation LINK_PUBLIC
  stacktrace
  base
  logger
  platform
  thread_name
  ${Boost_LIBRARIES}
  )

add_library(synchronization STATIC
  concurrency/synchronization
  )
add_dependencies(synchronization generate_error_codes generate_action_types install_tdb_h)

add_library(mongocommon_util STATIC
  background
  intrusive_counter
  util
  trace
  paths
  progress_meter
  percentage_progress_meter
  concurrency/task
  concurrency/thread_pool
  password
  concurrency/rwlockimpl
  histogram
  concurrency/spin_lock
  text_startuptest
  stack_introspect
  net/sock
  net/socket_poll
  net/ssl_manager
  net/ssl_options
  net/httpclient
  net/message
  net/message_port
  net/listen
  startup_test
  version
  )
add_dependencies(mongocommon_util generate_error_codes generate_action_types)
target_link_libraries(mongocommon_util LINK_PUBLIC
  bson
  foundation
  mongohasher
  md5
  processinfo
  synchronization
  stacktrace
  stringutils
  fail_point
  thread_name
  options_parser
  ${PCRE_LIBRARIES}
  murmurhash3
  ${Boost_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT}
  )

add_library(stringutils STATIC
  stringutils
  base64
  hex
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
target_link_libraries(processinfo LINK_PUBLIC
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
target_link_libraries(fail_point LINK_PUBLIC
  foundation
  bson
  )

# WONTFIX: ntservice, ntservice_test
