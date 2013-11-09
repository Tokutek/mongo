string(REPLACE "-Wnon-virtual-dtor" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
string(REPLACE "-Wall" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
string(REPLACE "-Wall" "" CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")

include_directories(include)

add_library(yaml STATIC
  src/binary
  src/graphbuilder
  src/graphbuilderadapter
  src/convert
  src/directives
  src/emit
  src/emitfromevents
  src/emitter
  src/emitterstate
  src/emitterutils
  src/exp
  src/memory
  src/node
  src/node_data
  src/nodebuilder
  src/nodeevents
  src/null
  src/ostream_wrapper
  src/parse
  src/parser
  src/regex
  src/scanner
  src/scanscalar
  src/scantag
  src/scantoken
  src/simplekey
  src/singledocparser
  src/stream
  src/tag
  )

set(YAML_FOUND TRUE CACHE BOOL "yaml found")
set(YAML_LIBRARIES yaml CACHE STRING "yaml libraries")
set(YAML_INCLUDE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/include" CACHE STRING "yaml include dirs")

mark_as_advanced(YAML_FOUND YAML_LIBRARIES YAML_INCLUDE_DIRS)
