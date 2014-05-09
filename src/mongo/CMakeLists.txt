include_directories(
  ..
  .
  )

if (USE_SASL_CLIENT)
  include_directories(${SASL_INCLUDE_DIRS})
endif ()

# for processinfo/systeminfo
if (APPLE)
  set(system_suffix darwin)
elseif (CMAKE_SYSTEM_NAME STREQUAL FreeBSD)
  set(system_suffix freebsd)
elseif (CMAKE_SYSTEM_NAME STREQUAL SunOS)
  set(system_suffix sunos5)
elseif (CMAKE_SYSTEM_NAME STREQUAL Linux)
  set(system_suffix linux2)
else ()
  message(SEND_ERROR "win32 compilation unsupported")
endif ()

# for error_codes and action_type
include_directories("${CMAKE_CURRENT_BINARY_DIR}/..")

set_source_files_properties(
  "${CMAKE_CURRENT_BINARY_DIR}/base/error_codes.cpp"
  PROPERTIES GENERATED TRUE)

if (JS_ENGINE STREQUAL v8)
  set(TOKUMX_JS_ENGINE V8)
elseif (JS_ENGINE STREQUAL sm)
  set(TOKUMX_JS_ENGINE SpiderMonkey)
else ()
  set(TOKUMX_JS_ENGINE Unknown)
endif ()
set(TOKUMX_LOADER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${CMAKE_STATIC_LINKER_FLAGS} ${CMAKE_EXE_LINKER_FLAGS}")
set(TOKUMX_COMPILER_FLAGS "${CMAKE_CXX_FLAGS}")
if (CMAKE_BUILD_TYPE STREQUAL Debug)
  set(TOKUMX_LOADER_FLAGS "${TOKUMX_LOADER_FLAGS} ${CMAKE_SHARED_LINKER_FLAGS_DEBUG} ${CMAKE_STATIC_LINKER_FLAGS_DEBUG} ${CMAKE_EXE_LINKER_FLAGS_DEBUG}")
  set(TOKUMX_COMPILER_FLAGS "${TOKUMX_COMPILER_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG}")
elseif (CMAKE_BUILD_TYPE STREQUAL Release)
  set(TOKUMX_LOADER_FLAGS "${TOKUMX_LOADER_FLAGS} ${CMAKE_SHARED_LINKER_FLAGS_RELEASE} ${CMAKE_STATIC_LINKER_FLAGS_RELEASE} ${CMAKE_EXE_LINKER_FLAGS_RELEASE}")
  set(TOKUMX_COMPILER_FLAGS "${TOKUMX_COMPILER_FLAGS} ${CMAKE_CXX_FLAGS_RELEASE}")
elseif (CMAKE_BUILD_TYPE STREQUAL RelWithDebInfo)
  set(TOKUMX_LOADER_FLAGS "${TOKUMX_LOADER_FLAGS} ${CMAKE_SHARED_LINKER_FLAGS_RELWITHDEBINFO} ${CMAKE_STATIC_LINKER_FLAGS_RELWITHDEBINFO} ${CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO}")
  set(TOKUMX_COMPILER_FLAGS "${TOKUMX_COMPILER_FLAGS} ${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
else ()
  message(WARNING "unknown build type")
endif ()

find_program(UNAME uname)
if (UNAME MATCHES NOTFOUND)
  set(SEND_ERROR "uname not found")
endif ()
execute_process(
  COMMAND ${UNAME} -a
  OUTPUT_VARIABLE TOKUMX_SYS_INFO
  RESULT_VARIABLE retval
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
  )
if (NOT retval EQUAL 0)
  message(WARNING "${UNAME}: error: ${retval}")
  set(TOKUMX_SYS_INFO "unknown")
endif ()
set_source_files_properties(
  "${CMAKE_CURRENT_BINARY_DIR}/buildinfo.cpp"
  PROPERTIES GENERATED TRUE)
configure_file(buildinfo.cpp.in buildinfo.cpp)

set_source_files_properties(
  "${CMAKE_CURRENT_BINARY_DIR}/shell/mongo.cpp"
  PROPERTIES GENERATED TRUE)
add_library(mongocommon STATIC
  pch
  "${CMAKE_CURRENT_BINARY_DIR}/buildinfo.cpp"
  db/lasterror
  "${CMAKE_CURRENT_BINARY_DIR}/shell/mongo.cpp"
  db/dbmessage
  )
add_dependencies(mongocommon generate_mongo_cpp generate_action_types)
target_link_libraries(mongocommon LINK_PUBLIC
  mongocommon_client
  mongocommon_util
  bson
  foundation
  stringutils
  fail_point
  ${Boost_LIBRARIES}
  )

add_subdirectory(base)
add_subdirectory(bson)
add_subdirectory(db)
add_subdirectory(platform)
add_subdirectory(plugins)
add_subdirectory(s)
add_subdirectory(scripting)
add_subdirectory(shell)
add_subdirectory(tools)
add_subdirectory(util)

set(client_sources
  base/configuration_variable_manager.cpp
  base/global_initializer.cpp
  base/global_initializer_registerer.cpp
  base/init.cpp
  base/initializer.cpp
  base/initializer_context.cpp
  base/initializer_dependency_graph.cpp
  base/make_string_vector.cpp
  base/parse_number.cpp
  base/status.cpp
  base/string_data.cpp
  bson/bson_validate.cpp
  bson/oid.cpp
  bson/util/bson_extract.cpp
  client/clientAndShell.cpp
  client/clientOnly.cpp
  client/connection_factory.cpp
  client/connpool.cpp
  client/dbclient.cpp
  client/dbclient_rs.cpp
  client/dbclientcursor.cpp
  client/gridfs.cpp
  client/model.cpp
  client/remote_loader.cpp
  client/remote_transaction.cpp
  client/sasl_client_authenticate.cpp
  client/syncclusterconnection.cpp
  db/jsobj.cpp
  db/json.cpp
  db/lasterror.cpp
  db/dbmessage.cpp
  pch.cpp
  platform/backtrace.cpp
  platform/posix_fadvise.cpp
  platform/random.cpp
  util/assert_util.cpp
  util/background.cpp
  util/base64.cpp
  util/concurrency/rwlockimpl.cpp
  util/concurrency/spin_lock.cpp
  util/concurrency/synchronization.cpp
  util/concurrency/task.cpp
  util/concurrency/thread_pool.cpp
  util/concurrency/mutexdebugger.cpp
  util/debug_util.cpp
  util/stacktrace.cpp
  util/fail_point.cpp
  util/fail_point_registry.cpp
  util/fail_point_service.cpp
  util/file.cpp
  util/histogram.cpp
  util/intrusive_counter.cpp
  util/log.cpp
  util/md5.cpp
  util/md5main.cpp
  util/net/httpclient.cpp
  util/net/listen.cpp
  util/net/message.cpp
  util/net/message_port.cpp
  util/net/sock.cpp
  util/net/ssl_manager.cpp
  util/password.cpp
  util/processinfo.cpp
  util/processinfo_${system_suffix}.cpp
  util/ramlog.cpp
  util/signal_handlers.cpp
  util/stringutils.cpp
  util/text.cpp
  util/time_support.cpp
  util/timer.cpp
  util/trace.cpp
  util/util.cpp
  util/version.cpp
  ../third_party/murmurhash3/MurmurHash3.cpp
  )

if (USE_SASL_CLIENT)
  list(APPEND client_sources
    client/sasl_client_authenticate_impl.cpp
    client/sasl_client_session.cpp
    )
endif ()
if (TOKUMX_CLIENT_LIB_SHARED)
  add_library(mongoclient SHARED
    "${CMAKE_CURRENT_BINARY_DIR}/base/error_codes.cpp"
    "${CMAKE_CURRENT_BINARY_DIR}/buildinfo.cpp"
    ${client_sources}
    )
  target_link_libraries(mongoclient
    ${Boost_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    )
  if (USE_SASL_CLIENT)
    target_link_libraries(mongoclient
      ${SASL_LIBRARIES}
      )
  endif ()
  if (CMAKE_SYSTEM_NAME STREQUAL Linux)
    set_target_properties(mongoclient PROPERTIES
      LINK_FLAGS "-Wl,--as-needed -Wl,-zdefs"
      )
  endif ()
else ()
  add_library(mongoclient STATIC
    "${CMAKE_CURRENT_BINARY_DIR}/base/error_codes.cpp"
    "${CMAKE_CURRENT_BINARY_DIR}/buildinfo.cpp"
    ${client_sources}
    )
  target_link_libraries(mongoclient LINK_PUBLIC
    ${Boost_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    )
  if (USE_SASL_CLIENT)
    target_link_libraries(mongoclient LINK_PUBLIC
      ${SASL_LIBRARIES}
      )
  endif ()
endif ()
add_dependencies(mongoclient generate_error_codes generate_action_types install_tdb_h)
install(TARGETS mongoclient
  DESTINATION ${INSTALL_LIBDIR}
  COMPONENT tokumx_client_libs
  )

foreach (src
    ${client_sources}
    bson/bsondemo/bsondemo.cpp
    )
  get_filename_component(dir ${src} PATH)
  install(FILES ${src}
    DESTINATION src/mongo/${dir}
    COMPONENT tokumx_client_sources
    )
endforeach ()
if (NOT USE_SASL_CLIENT)
  # install sources anyway
  install(FILES
    client/sasl_client_authenticate_impl.cpp
    client/sasl_client_session.cpp
    DESTINATION src/mongo/client
    COMPONENT tokumx_client_sources
    )
endif ()
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/base/error_codes.h"
  DESTINATION include/mongo/base
  COMPONENT tokumx_client_headers
  )
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/db/auth/action_type.h"
  DESTINATION include/mongo/db/auth
  COMPONENT tokumx_client_headers
  )
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/base/error_codes.cpp"
  base/generate_error_codes.py
  base/error_codes.err
  DESTINATION src/mongo/base
  COMPONENT tokumx_client_sources
  )
install(FILES
  db/auth/generate_action_types.py
  db/auth/action_types.txt
  DESTINATION src/mongo/db/auth
  COMPONENT tokumx_client_sources
  )
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/buildinfo.cpp"
  DESTINATION src/mongo
  COMPONENT tokumx_client_sources
  )

foreach (client_header_dir
    base
    bson
    bson/util
    client
    db
    db/auth
    db/stats
    db/repl
    db/ops
    db/storage
    platform
    s
    scripting
    shell
    util
    util/concurrency
    util/mongoutils
    util/net
    .
    )
  file(GLOB headers
    ${client_header_dir}/*.h
    ${client_header_dir}/*.hpp
    )
  install(FILES ${headers}
    DESTINATION include/mongo/${client_header_dir}
    COMPONENT tokumx_client_headers
    )
endforeach ()

add_subdirectory(client)

if (BUILD_TESTING)
  add_subdirectory(dbtests)
  add_subdirectory(unittest)
endif ()

# In order for link_recursive_deps to be able to resolve the full dependency tree, all the libraries
# need to be defined before we call it, so all the test executables have to come after all the above
# subdirectories.
if (BUILD_TESTING)
  foreach (test
      configuration_variable_manager_test
      initializer_dependency_graph_test
      initializer_test
      owned_pointer_vector_test
      owned_pointer_map_test
      parse_number_test
      status_test
      string_data_test
      units_test
      )
    add_executable(${test} base/${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    link_recursive_deps(${test}
      COMBINED_LIBNAME basic_unittest_deps
      unittest_main
      unittest_crutch
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()

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
    add_executable(${test} bson/${test_source})
    add_dependencies(${test} generate_error_codes generate_action_types)
    link_recursive_deps(${test}
      COMBINED_LIBNAME bson_unittest_deps
      bson
      unittest_main
      unittest_crutch
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()

  foreach (test
      dbclient_rs_test
      scoped_db_conn_test
      )
    add_executable(${test} client/${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    link_recursive_deps(${test}
      COMBINED_LIBNAME client_rs_unittest_deps
      unittest_main
      coredb
      coreserver
      coreshard
      dbcmdline
      mongocommon
      mongodandmongos
      mongoscore
      ${TokuKV_LIBRARIES}
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()
  target_link_libraries(dbclient_rs_test
    mocklib
    )

  foreach (test
      cmdline_test
      field_ref_test
      index_set_test
      server_parameters_test
      )
    add_executable(${test} db/${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    link_recursive_deps(${test}
      COMBINED_LIBNAME basic_unittest_deps
      unittest_main
      unittest_crutch
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()

  target_link_whole_libraries(cmdline_test dbcmdline)
  target_link_whole_libraries(field_ref_test db_common)
  target_link_whole_libraries(index_set_test bson index_set)
  target_link_whole_libraries(server_parameters_test server_parameters)

  foreach (test
      atomic_word_test
      bits_test
      random_test
      )
    add_executable(${test} platform/${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    link_recursive_deps(${test}
      COMBINED_LIBNAME basic_unittest_deps
      unittest_main
      unittest_crutch
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()

  foreach (test
      chunk_version_test
      field_parser_test
      mongo_version_range_test
      type_changelog_test
      type_chunk_test
      type_collection_test
      type_config_version_test
      type_database_test
      type_locks_test
      type_lockpings_test
      type_settings_test
      type_shard_test
      type_mongos_test
      type_tags_test
      )
    add_executable(${test} s/${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    link_recursive_deps(${test}
      COMBINED_LIBNAME sharding_unittest_deps
      s_base
      unittest_main
      unittest_crutch
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()

  target_link_libraries(mongo_version_range_test bson)

  # no crutch
  foreach (test
      chunk_diff_test
      collection_manager_test
      metadata_loader_test
      )
    add_executable(${test} s/${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    link_recursive_deps(${test}
      COMBINED_LIBNAME sharding_nocrutch_unittest_deps
      unittest_main
      mongocommon
      s_metadata
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()

  foreach (test
      balancer_policy_tests
      shard_conn_test
      shard_test
      )
    add_executable(${test} s/${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    link_recursive_deps(${test}
      COMBINED_LIBNAME sharding_other_unittest_deps
      unittest_main
      mongoscore
      coreshard
      mongocommon
      coreserver
      coredb
      dbcmdline
      mongodandmongos
      ${TokuKV_LIBRARIES}
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()
  target_link_whole_libraries(collection_manager_test mocklib)
  target_link_whole_libraries(metadata_loader_test mocklib)
  target_link_whole_libraries(shard_conn_test mocklib)

  foreach (test
      bson_template_evaluator_test
      v8_deadline_monitor_test
      )
    add_executable(${test} scripting/${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    target_link_libraries(${test}
      unittest_main
      foundation
      unittest_crutch
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()
  
  target_link_libraries(bson_template_evaluator_test bson_template_evaluator)

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
    add_executable(${test} util/${test_source})
    add_dependencies(${test} generate_error_codes generate_action_types)
    link_recursive_deps(${test}
      COMBINED_LIBNAME basic_unittest_deps
      unittest_main
      unittest_crutch
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
  add_executable(sock_test util/net/sock_test)
  add_dependencies(sock_test generate_error_codes generate_action_types)
  link_recursive_deps(sock_test
    unittest_main
    mongocommon
    notmongodormongos
    ${TOKUMX_SSL_LIBRARIES}
    )

  add_executable(md5_test util/md5_test util/md5main)
  add_dependencies(md5_test generate_error_codes generate_action_types)
  link_recursive_deps(md5_test
    COMBINED_LIBNAME basic_unittest_deps
    unittest_main
    unittest_crutch
    ${TOKUMX_SSL_LIBRARIES}
    )
  target_link_whole_libraries(md5_test
    md5
    )
endif ()

add_executable(mongod
  db/db
  db/commands/touch
  )
add_dependencies(mongod generate_error_codes generate_action_types install_tdb_h)

target_link_libraries(mongod
  ${TokuBackup_LIBRARIES}
  )
if (NOT APPLE)
  target_link_whole_libraries(mongod
    jemalloc
    )
endif ()
link_recursive_deps(mongod
  coredb
  coreserver
  mongodandmongos
  serveronly
  ${TOKUMX_SSL_LIBRARIES}
  )

foreach (tool
    dump
    restore
    export
    import
    stat
    top
    2toku
    files
    bridge
    )
  add_executable(mongo${tool} tools/${tool})
endforeach ()
add_executable(bsondump tools/bsondump)
foreach (tool
    mongodump
    mongorestore
    mongoexport
    mongoimport
    mongostat
    mongotop
    mongo2toku
    mongofiles
    mongobridge
    bsondump
    )
  add_dependencies(${tool} generate_error_codes generate_action_types install_tdb_h)
  if (NOT APPLE)
    target_link_whole_libraries(${tool}
      jemalloc
      )
  endif ()
  link_recursive_deps(${tool}
    COMBINED_LIBNAME all_tool_deps
    alltools
    ${TOKUMX_SSL_LIBRARIES}
    )
endforeach ()
install_executables(tokumx_tools
  mongodump
  mongorestore
  mongoexport
  mongoimport
  mongostat
  mongotop
  mongo2toku
  mongofiles
  bsondump
  )
target_link_libraries(mongofiles gridfs)

add_executable(docgen tools/docgeneratormain)
add_executable(loadgen tools/loadgenerator)
foreach (tool
    docgen
    loadgen
    )
  add_dependencies(${tool} generate_error_codes generate_action_types)
  if (NOT APPLE)
    target_link_whole_libraries(${tool}
      jemalloc
      )
  endif ()
  link_recursive_deps(${tool}
    COMBINED_LIBNAME docgen_tool_deps
    alltools
    docgenerator
    ${TOKUMX_SSL_LIBRARIES}
    )
endforeach ()

add_executable(mongos
  s/server
  )
add_dependencies(mongos generate_error_codes generate_action_types)
if (NOT APPLE)
  target_link_whole_libraries(mongos
    jemalloc
    )
endif ()
link_recursive_deps(mongos
  mongoscore
  coreserver
  coredb
  mongocommon
  coreshard
  dbcmdline
  mongodandmongos
  s_upgrade
  ${TOKUMX_SSL_LIBRARIES}
  )

if (APPLE OR PCAP_FOUND)
  add_executable(mongosniff tools/sniffer)
  add_dependencies(mongosniff generate_error_codes generate_action_types)
  set_target_properties(mongosniff PROPERTIES
    COMPILE_DEFINITIONS MONGO_EXPOSE_MACROS
    )
  if (NOT APPLE)
    target_link_whole_libraries(mongosniff
      jemalloc
      )
  endif ()
  link_recursive_deps(mongosniff
    gridfs
    serveronly
    coreserver
    coredb
    notmongodormongos
    ${PCAP_LIBRARIES}
    ${TOKUMX_SSL_LIBRARIES}
    )
  install_executables(tokumx_tools
    mongosniff
    )
endif ()

remove_as_needed(mongod mongos)

set_source_files_properties(
  "${CMAKE_CURRENT_BINARY_DIR}/shell/mongo-server.cpp"
  PROPERTIES GENERATED TRUE)
add_executable(mongo
  shell/dbshell
  shell/shell_utils
  shell/shell_utils_extended
  shell/shell_utils_launcher
  "${CMAKE_CURRENT_BINARY_DIR}/shell/mongo-server.cpp"
  shell/linenoise
  shell/linenoise_utf8
  shell/mk_wcwidth
  )
add_dependencies(mongo generate_mongo_server_cpp generate_error_codes generate_action_types)
link_recursive_deps(mongo
  clientandshell
  mongocommon
  scripting
  ${PCRE_LIBRARIES}
  ${TOKUMX_SSL_LIBRARIES}
  )
install_executables(tokumx_tools
  mongo
  )
install_executables(tokumx_server
  mongos
  mongod
  )

if (BUILD_TESTING)
  add_test(NAME mongo/shell/test_shell
    COMMAND "${PYTHON2}"
      "${CMAKE_CURRENT_SOURCE_DIR}/../../buildscripts/test_shell.py"
      "$<TARGET_FILE:mongo>"
    )
endif ()

if (BUILD_TESTING)
  set(js_cxx_ctest_root "${CMAKE_BINARY_DIR}/ctest/mongo/cxx")
  set(js_cxx_dbpath "${js_cxx_ctest_root}/db")
  set(js_cxx_logpath "${js_cxx_ctest_root}/log")
  file(MAKE_DIRECTORY "${js_cxx_dbpath}")
  add_test(
    NAME mongo/cxx/clean-mongod-dbpath
    COMMAND bash -c "ls ${js_cxx_dbpath}/* | xargs rm -rf"
    )
  add_test(
    NAME mongo/cxx/start-mongod
    COMMAND $<TARGET_FILE:mongod>
    --dbpath "${js_cxx_dbpath}"
    --fork
    --logpath "${js_cxx_logpath}"
    --setParameter enableTestCommands=1
    )
  set_tests_properties(mongo/cxx/start-mongod PROPERTIES
    DEPENDS mongo/cxx/clean-mongod-dbpath
    )
  add_test(
    NAME mongo/cxx/check-mongod
    COMMAND /bin/bash -c "for i in `seq 1 180`; do if $<TARGET_FILE:mongo> --eval 'db.version()'; then break; fi; if [ $i = 180 ]; then exit 1; fi; sleep 1; done"
    )
  set_tests_properties(mongo/cxx/check-mongod PROPERTIES
    DEPENDS mongo/cxx/start-mongod
    )
  add_test(
    NAME mongo/cxx/stop-mongod
    COMMAND $<TARGET_FILE:mongo>
    --eval "db.adminCommand({shutdown: 1, force: true})"
    )
  set_tests_properties(mongo/cxx/stop-mongod PROPERTIES
    WILL_FAIL TRUE
    DEPENDS mongo/cxx/check-mongod
    )

  function (add_mongo_test section name)
    add_test(
      NAME mongo/cxx/${section}/${name}
      COMMAND ${ARGN}
      )
    set_tests_properties(mongo/cxx/${section}/${name} PROPERTIES
      DEPENDS mongo/cxx/check-mongod
      )
    set_property(TEST mongo/cxx/stop-mongod APPEND PROPERTY
      DEPENDS mongo/cxx/${section}/${name}
      )
  endfunction ()

  foreach (test
      configuration_variable_manager_test
      initializer_dependency_graph_test
      initializer_test
      owned_pointer_vector_test
      owned_pointer_map_test
      parse_number_test
      status_test
      string_data_test
      units_test
      )
    add_mongo_test(base ${test} ${test})
  endforeach ()

  foreach (test
      builder_test
      bson_extract_test
      mutable_bson_test
      mutable_bson_heap_test
      mutable_bson_builder_test
      mutable_bson_algo_test
      bson_field_test
      bson_validate_test
      )
    add_mongo_test(bson ${test} ${test})
  endforeach ()

  foreach (test
      atomic_word_test
      bits_test
      random_test
      )
    add_mongo_test(platform ${test} ${test})
  endforeach ()

  foreach (test
      balancer_policy_tests
      chunk_diff_test
      chunk_version_test
      collection_manager_test
      field_parser_test
      metadata_loader_test
      mongo_version_range_test
      shard_conn_test
      shard_test
      type_changelog_test
      type_chunk_test
      type_collection_test
      type_config_version_test
      type_database_test
      type_lockpings_test
      type_locks_test
      type_mongos_test
      type_settings_test
      type_shard_test
      type_tags_test
      )
    add_mongo_test(s ${test} ${test})
  endforeach ()

  foreach (test
      partitioned_counter_test
      descriptive_stats_test
      fail_point_test
      md5_test
      processinfo_test
      safe_num_test
      sock_test
      string_map_test
      stringutils_test
      text_test
      )
    add_mongo_test(util ${test} ${test})
  endforeach ()

  foreach (test
      action_set_test
      principal_set_test
      privilege_set_test
      authorization_manager_test
      )
    add_mongo_test(db/auth ${test} ${test})
  endforeach ()

  foreach (test
      cmdline_test
      field_ref_test
      index_set_test
      server_parameters_test
      )
    add_mongo_test(db ${test} ${test})
  endforeach ()

  foreach (test
      unittest_test
      fixture_test
      )
    add_mongo_test(unittest ${test} ${test})
  endforeach ()

  foreach (test
      authTest
      clientTest
      dbclient_rs_test
      firstExample
      httpClientTest
      scoped_db_conn_test
      secondExample
      tutorial
      whereExample
      )
    add_mongo_test(client ${test} ${test})
  endforeach ()

  foreach (test
      bson_template_evaluator_test
      v8_deadline_monitor_test
      )
    add_mongo_test(scripting ${test} ${test})
  endforeach ()

  add_mongo_test(dbtests smokeTest smokeTest)
endif ()
