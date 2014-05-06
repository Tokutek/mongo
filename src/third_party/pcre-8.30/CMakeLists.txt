set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS HAVE_CONFIG_H)

include_directories(.)

add_library(pcrecpp STATIC
  pcre_byte_order
  pcre_compile
  pcre_config
  pcre_dfa_exec
  pcre_exec
  pcre_fullinfo.c
  pcre_get.c
  pcre_globals.c
  pcre_maketables.c
  pcre_newline.c
  pcre_ord2utf8.c
  pcre_refcount.c
  pcre_string_utils.c
  pcre_study.c
  pcre_tables.c
  pcre_ucd.c
  pcre_valid_utf8.c
  pcre_version.c
  pcre_xclass.c
  # pcre nodist
  pcre_chartables.c
  # pcre cpp
  pcrecpp.cc
  pcre_scanner.cc
  pcre_stringpiece.cc
  # pcre posix
  pcreposix.c
  )

set(PCRE_FOUND TRUE CACHE BOOL "pcre found")
set(PCRE_LIBRARIES pcrecpp CACHE STRING "pcre libraries")
set(PCRE_INCLUDE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}" CACHE STRING "pcre include dirs")

mark_as_advanced(PCRE_FOUND PCRE_LIBRARIES PCRE_INCLUDE_DIRS)
