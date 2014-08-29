file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
set(MONGO_JS_SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/assert.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/types.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/utils.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/utils_sh.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/db.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/mongo.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/mr.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/query.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/collection.js"
  )
add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/mongo.cpp"
  COMMAND
    "${PYTHON2}"
        "${CMAKE_CURRENT_SOURCE_DIR}/jsheader-wrap.py"
        "${CMAKE_CURRENT_SOURCE_DIR}/../../../site_scons/site_tools"
        "${CMAKE_CURRENT_BINARY_DIR}/mongo.cpp"
        ${MONGO_JS_SOURCES}
  DEPENDS
    ${MONGO_JS_SOURCES}
  )
add_custom_target(generate_mongo_cpp DEPENDS
  "${CMAKE_CURRENT_BINARY_DIR}/mongo.cpp"
  )

set(MONGO_SERVER_JS_SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/servers.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/shardingtest.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/servers_misc.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/replsettest.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/replsetbridge.js"
  )
add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/mongo-server.cpp"
  COMMAND
    "${PYTHON2}"
        "${CMAKE_CURRENT_SOURCE_DIR}/jsheader-wrap.py"
        "${CMAKE_CURRENT_SOURCE_DIR}/../../../site_scons/site_tools"
        "${CMAKE_CURRENT_BINARY_DIR}/mongo-server.cpp"
        ${MONGO_SERVER_JS_SOURCES}
  DEPENDS
    ${MONGO_SERVER_JS_SOURCES}
  )
add_custom_target(generate_mongo_server_cpp DEPENDS
  "${CMAKE_CURRENT_BINARY_DIR}/mongo-server.cpp"
  )

set_source_files_properties(
  "${CMAKE_CURRENT_BINARY_DIR}/shell/mongo-server.cpp"
  PROPERTIES GENERATED TRUE)

add_library(shell_core STATIC
  shell_utils
  shell_utils_extended
  shell_utils_launcher
  "${CMAKE_CURRENT_BINARY_DIR}/mongo-server.cpp"
  linenoise
  linenoise_utf8
  mk_wcwidth
  shell_options_init
  )
add_dependencies(shell_core generate_mongo_server_cpp generate_error_codes generate_action_types)
target_link_libraries(shell_core LINK_PUBLIC
  clientandshell
  scripting
  mongocommon
  )

add_library(shell_options STATIC
  shell_options
  )
add_dependencies(shell_options generate_error_codes generate_action_types)
target_link_libraries(shell_options LINK_PUBLIC
  options_parser_init
  )
