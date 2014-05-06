find_path(SASL_INCLUDE_DIR NAMES sasl/sasl.h)
find_library(SASL_LIBRARY NAMES sasl2)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SASL DEFAULT_MSG
  SASL_LIBRARY SASL_INCLUDE_DIR)

if (SASL_FOUND)
  set(SASL_LIBRARIES ${SASL_LIBRARY})
  set(SASL_INCLUDE_DIRS ${SASL_INCLUDE_DIR})
else ()
  set(SASL_LIBRARIES)
  set(SASL_INCLUDE_DIRS)
endif ()

mark_as_advanced(SASL_LIBRARIES SASL_INCLUDE_DIRS)