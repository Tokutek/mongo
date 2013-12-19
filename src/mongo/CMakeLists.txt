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

find_program(GIT git)
if (GIT MATCHES NOTFOUND)
  set(SEND_ERROR "git not found")
endif ()
execute_process(
  COMMAND ${GIT} rev-parse HEAD
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../.."
  OUTPUT_VARIABLE TOKUMX_GIT_VERSION
  RESULT_VARIABLE retval
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
  )
if (NOT retval EQUAL 0)
  message(WARNING "${GIT}: error: ${retval}")
  set(TOKUMX_GIT_VERSION "unknown")
endif ()
execute_process(
  COMMAND ${GIT} rev-parse HEAD
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../third_party/ft-index"
  OUTPUT_VARIABLE TOKUKV_GIT_VERSION
  RESULT_VARIABLE retval
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
  )
if (NOT retval EQUAL 0)
  message(WARNING "${GIT}: error: ${retval}")
  set(TOKUKV_GIT_VERSION "unknown")
endif ()
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

add_subdirectory(base)
add_subdirectory(bson)
add_subdirectory(client)
add_subdirectory(db)
add_subdirectory(platform)
add_subdirectory(plugins)
add_subdirectory(s)
add_subdirectory(scripting)
add_subdirectory(shell)
add_subdirectory(tools)
add_subdirectory(util)

if (BUILD_TESTING)
  add_subdirectory(dbtests)
  add_subdirectory(unittest)
endif ()

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
target_link_libraries(mongocommon LINK_PRIVATE
  mongocommon_client
  mongocommon_util
  bson
  foundation
  stringutils
  fail_point
  ${Boost_LIBRARIES}
  )

add_executable(mongod
  db/db
  db/commands/touch
  )
add_dependencies(mongod generate_error_codes generate_action_types)

target_link_libraries(mongod
  ${TokuBackup_LIBRARIES}
  )
target_link_whole_libraries(mongod
  coredb
  coreserver
  mongodandmongos
  serveronly
  base
  )
target_link_libraries(mongod
  ${TokuKV_LIBRARIES}
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
  add_dependencies(mongo${tool} generate_error_codes generate_action_types)
  target_link_whole_libraries(mongo${tool}
    alltools
    base
    )
  target_link_libraries(mongo${tool}
    ${TokuKV_LIBRARIES}
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
  )
target_link_libraries(mongofiles gridfs)

add_executable(docgen tools/docgeneratormain)
add_executable(loadgen tools/loadgenerator)
add_executable(bsondump tools/bsondump)
foreach (tool
    docgen
    loadgen
    bsondump
    )
  add_dependencies(${tool} generate_error_codes generate_action_types)
  target_link_whole_libraries(${tool}
    alltools
    base
    )
  target_link_libraries(${tool}
    ${TokuKV_LIBRARIES}
    )
endforeach ()
install_executables(tokumx_tools bsondump)
target_link_libraries(docgen docgenerator)
target_link_libraries(loadgen docgenerator)

add_executable(mongos
  s/server
  )
add_dependencies(mongos generate_error_codes generate_action_types)
target_link_whole_libraries(mongos
  mongoscore
  coreserver
  coredb
  mongocommon
  coreshard
  dbcmdline
  mongodandmongos
  s_upgrade
  )
target_link_libraries(mongos
  ${TokuKV_LIBRARIES}
  )

if (APPLE OR PCAP_FOUND)
  add_executable(mongosniff tools/sniffer)
  add_dependencies(mongosniff generate_error_codes generate_action_types)
  set_target_properties(mongosniff PROPERTIES
    COMPILE_DEFINITIONS MONGO_EXPOSE_MACROS
    )
  target_link_whole_libraries(mongosniff
    gridfs
    serveronly
    coreserver
    coredb
    notmongodormongos
    base
    )
  target_link_libraries(mongosniff
    ${PCAP_LIBRARIES}
    ${TokuKV_LIBRARIES}
    )
endif ()

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
target_link_whole_libraries(mongo
  clientandshell
  mongocommon
  mongocommon_client
  scripting
  base
  )
target_link_libraries(mongo
  ${PCRE_LIBRARIES}
  )
install_executables(tokumx_server
  mongo
  mongos
  mongod
  )

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
add_library(mongoclient STATIC
  "${CMAKE_CURRENT_BINARY_DIR}/base/error_codes.cpp"
  "${CMAKE_CURRENT_BINARY_DIR}/buildinfo.cpp"
  ${client_sources})
add_dependencies(mongoclient generate_error_codes generate_action_types)
target_link_libraries(mongoclient LINK_PRIVATE
  ${Boost_LIBRARIES}
  )
if (USE_SASL_CLIENT)
  target_link_libraries(mongoclient LINK_PRIVATE
    ${SASL_LIBRARIES}
    )
endif ()

foreach (src ${client_sources})
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
  "${CMAKE_CURRENT_BINARY_DIR}/base/error_codes.cpp"
  "${CMAKE_CURRENT_BINARY_DIR}/base/error_codes.h"
  DESTINATION src/mongo/base
  COMPONENT tokumx_client_sources
  )
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/db/auth/action_type.h"
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
    DESTINATION src/mongo/${client_header_dir}
    COMPONENT tokumx_client_headers
    )
endforeach ()

find_library(GCC_S_LIBRARY NAMES gcc_s PATHS
  /lib64
  /lib/x86_64-linux-gnu
  /usr/local/gcc-4.7/lib64
  /usr/local/gcc-4.7/lib/x86_64-linux-gnu
  /usr/local/gcc-4.7.3/lib64
  /usr/local/gcc-4.7.3/lib/x86_64-linux-gnu
  )
if (NOT GCC_S_LIBRARY MATCHES NOTFOUND)
  get_filename_component(GCC_S_LIBRARY_DIR "${GCC_S_LIBRARY}" PATH)
  file(GLOB GCC_S_LIBRARY_FILES "${GCC_S_LIBRARY_DIR}/libgcc_s.so*")
  install(FILES ${GCC_S_LIBRARY_FILES}
    DESTINATION ${INSTALL_LIBDIR}
    COMPONENT tokumx_libs_extra
    )
endif ()
find_library(STDCXX_LIBRARY NAMES stdc++ PATHS
  /usr/lib64
  /usr/lib/x86_64-linux-gnu
  /usr/local/gcc-4.7/lib64
  /usr/local/gcc-4.7/lib/x86_64-linux-gnu
  /usr/local/gcc-4.7.3/lib64
  /usr/local/gcc-4.7.3/lib/x86_64-linux-gnu
  )
if (NOT STDCXX_LIBRARY MATCHES NOTFOUND)
  get_filename_component(STDCXX_LIBRARY_DIR "${STDCXX_LIBRARY}" PATH)
  file(GLOB STDCXX_LIBRARY_FILES "${STDCXX_LIBRARY_DIR}/libstdc++.so*")
  install(FILES ${STDCXX_LIBRARY_FILES}
    DESTINATION ${INSTALL_LIBDIR}
    COMPONENT tokumx_libs_extra
    )
endif ()
