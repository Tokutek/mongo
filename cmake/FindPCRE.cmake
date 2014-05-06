find_path(PCRE_INCLUDE_DIR pcre.h)
find_library(PCRE_LIBRARY NAMES pcre)
find_library(PCRECPP_LIBRARY NAMES pcrecpp)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PCRE DEFAULT_MSG
  PCRE_LIBRARY PCRECPP_LIBRARY PCRE_INCLUDE_DIR)

if (PCRE_FOUND)
  set(PCRE_LIBRARIES ${PCRE_LIBRARY} ${PCRECPP_LIBRARY})
  set(PCRE_INCLUDE_DIRS ${PCRE_INCLUDE_DIR})
else ()
  set(PCRE_LIBRARIES)
  set(PCRE_INCLUDE_DIRS)
endif ()

mark_as_advanced(PCRE_LIBRARIES PCRE_INCLUDE_DIRS)