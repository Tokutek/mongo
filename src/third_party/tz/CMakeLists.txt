add_library(tz STATIC
  timegm
  )

set(TZ_FOUND TRUE CACHE BOOL "tz found")
set(TZ_LIBRARIES tz CACHE STRING "tz libraries")
set(TZ_INCLUDE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}" CACHE STRING "tz include dirs")

mark_as_advanced(TZ_FOUND TZ_LIBRARIES TZ_INCLUDE_DIRS)
