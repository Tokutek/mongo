include_directories(.)
include_directories(../..)
include_directories("${CMAKE_CURRENT_BINARY_DIR}/../..")

add_subdirectory(base)
add_subdirectory(strings)
add_subdirectory(util/coding)
add_subdirectory(util/math)

set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS
  DEBUG_MODE=false
  )

add_library(s2 STATIC
  s1angle
  s2
  s2cellid
  s2latlng
  s1interval
  s2cap
  s2cell
  s2cellunion
  s2edgeindex
  s2edgeutil
  s2latlngrect
  s2loop
  s2pointregion
  s2polygon
  s2polygonbuilder
  s2polyline
  s2r2rect
  s2region
  s2regioncoverer
  s2regionintersection
  s2regionunion
  )
target_link_libraries(s2 LINK_PUBLIC
  s2_base
  s2_strings
  s2_coding
  s2_math
  )

set(S2_FOUND TRUE CACHE BOOL "s2 found")
set(S2_LIBRARIES s2 CACHE STRING "s2 libraries")
set(S2_INCLUDE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}" CACHE STRING "s2 include dirs")

mark_as_advanced(S2_FOUND S2_LIBRARIES S2_INCLUDE_DIRS)