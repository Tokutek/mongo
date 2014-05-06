include_directories("${CMAKE_CURRENT_SOURCE_DIR}")

add_library(boost_system STATIC libs/system/src/error_code)
set(Boost_system_FOUND TRUE CACHE BOOL "boost_system found")
set(Boost_system_LIBRARY boost_system CACHE STRING "boost_system library")

if (UNIX)
  set(boost_thread_srcs
    libs/thread/src/pthread/once
    libs/thread/src/pthread/thread
    )
else ()
  set(boost_thread_srcs
    libs/thread/src/win32/thread
    libs/thread/src/win32/tss_dll
    libs/thread/src/win32/tss_pe
    )
endif ()

add_library(boost_thread STATIC ${boost_thread_srcs})
target_link_libraries(boost_thread LINK_PUBLIC ${CMAKE_THREAD_LIBS_INIT})
set(Boost_thread_FOUND TRUE CACHE BOOL "boost_thread found")
set(Boost_thread_LIBRARY boost_thread CACHE STRING "boost_thread library")

add_library(boost_filesystem STATIC
  libs/filesystem/v2/src/v2_operations
  libs/filesystem/v2/src/v2_path
  libs/filesystem/v2/src/v2_portability
  libs/filesystem/v3/src/codecvt_error_category
  libs/filesystem/v3/src/operations
  libs/filesystem/v3/src/path
  libs/filesystem/v3/src/path_traits
  libs/filesystem/v3/src/portability
  libs/filesystem/v3/src/unique_path
  libs/filesystem/v3/src/utf8_codecvt_facet
  libs/filesystem/v3/src/windows_file_codecvt
  )
set(Boost_filesystem_FOUND TRUE CACHE BOOL "boost_filesystem found")
set(Boost_filesystem_LIBRARY boost_filesystem CACHE STRING "boost_filesystem library")

add_library(boost_program_options STATIC
  libs/program_options/src/cmdline.cpp
  libs/program_options/src/config_file.cpp
  libs/program_options/src/convert.cpp
  libs/program_options/src/options_description.cpp
  libs/program_options/src/parsers.cpp
  libs/program_options/src/positional_options.cpp
  libs/program_options/src/split.cpp
  libs/program_options/src/utf8_codecvt_facet.cpp
  libs/program_options/src/value_semantic.cpp
  libs/program_options/src/variables_map.cpp
  libs/program_options/src/winmain.cpp
  )
set(Boost_program_options_FOUND TRUE CACHE BOOL "boost_program_options found")
set(Boost_program_options_LIBRARY boost_program_options CACHE STRING "boost_program_options library")


set(Boost_FOUND TRUE CACHE BOOL "boost found")
set(Boost_INCLUDE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}" CACHE STRING "boost include dirs")
set(Boost_LIBRARIES boost_system boost_thread boost_filesystem boost_program_options CACHE STRING "boost libraries")
set(Boost_LIBRARY_DIRS "${CMAKE_CURRENT_BINARY_DIR}" CACHE STRING "boost library dirs")
set(Boost_VERSION 104900 CACHE STRING "boost version")
set(Boost_LIB_VERSION 1_49 CACHE STRING "boost lib version")
set(Boost_MAJOR_VERSION 1 CACHE STRING "boost major version")
set(Boost_MINOR_VERSION 49 CACHE STRING "boost minor version")
set(Boost_SUBMINOR_VERSION 0 CACHE STRING "boost subminor version")

mark_as_advanced(
  Boost_system_FOUND
  Boost_system_LIBRARY
  Boost_thread_FOUND
  Boost_thread_LIBRARY
  Boost_filesystem_FOUND
  Boost_filesystem_LIBRARY
  Boost_program_options_FOUND
  Boost_program_options_LIBRARY
  Boost_FOUND
  Boost_INCLUDE_DIRS
  Boost_LIBRARIES
  Boost_LIBRARY_DIRS
  Boost_VERSION
  Boost_LIB_VERSION
  Boost_MAJOR_VERSION
  Boost_MINOR_VERSION
  Boost_SUBMINOR_VERSION
  )