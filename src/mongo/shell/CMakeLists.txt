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

