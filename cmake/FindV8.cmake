find_path(V8_INCLUDE_DIR NAMES v8.h)
find_library(V8_LIBRARY NAMES v8)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(V8 DEFAULT_MSG
  V8_LIBRARY V8_INCLUDE_DIR)

if (V8_FOUND)
  set(V8_LIBRARIES ${V8_LIBRARY})
  set(V8_INCLUDE_DIRS ${V8_INCLUDE_DIR})
else ()
  set(V8_LIBRARIES)
  set(V8_INCLUDE_DIRS)
endif ()

mark_as_advanced(V8_LIBRARIES V8_INCLUDE_DIRS)