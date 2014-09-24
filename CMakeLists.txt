if (APPLE)
  cmake_minimum_required(VERSION 2.8.12 FATAL_ERROR)
  set(CMAKE_MACOSX_RPATH TRUE)
else ()
  cmake_minimum_required(VERSION 2.8.9 FATAL_ERROR)
endif ()

if (POLICY CMP0026)
  cmake_policy(SET CMP0026 OLD)
endif ()
if (POLICY CMP0043)
  cmake_policy(SET CMP0043 OLD)
endif ()
if (POLICY CMP0045)
  cmake_policy(SET CMP0045 OLD)
endif ()

project(tokumx)

## Versions of gcc >= 4.9.0 require special version of 'ar' and 'ranlib' for
## link-time optimizations to work properly.
##
## From https://gcc.gnu.org/gcc-4.9/changes.html:
##
##     When using a linker plugin, compiling with the -flto option now
##     generates slim objects files (.o) which only contain intermediate
##     language representation for LTO. Use -ffat-lto-objects to create
##     files which contain additionally the object code. To generate
##     static libraries suitable for LTO processing, use gcc-ar and
##     gcc-ranlib; to list symbols from a slim object file use
##     gcc-nm. (Requires that ar, ranlib and nm have been compiled with
##     plugin support.)
if ((CMAKE_CXX_COMPILER_ID STREQUAL GNU) AND
    NOT (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.9.0"))
  find_program(gcc_ar "gcc-ar")
  if (gcc_ar)
    set(CMAKE_AR "${gcc_ar}")
  endif ()
  find_program(gcc_ranlib "gcc-ranlib")
  if (gcc_ranlib)
    set(CMAKE_RANLIB "${gcc_ranlib}")
  endif ()
endif()

option(USE_SSL "Enable SSL" OFF)
set(JS_ENGINE v8 CACHE STRING "Javascript engine (sm or v8)")
option(USE_SYSTEM_V8 "use system version of v8 library" OFF)
option(USE_SYSTEM_PCRE "use system version of pcre library" OFF)
option(USE_SYSTEM_S2 "use system version of s2 library" OFF)
option(USE_SYSTEM_BOOST "use system version of boost libraries" OFF)
option(USE_SASL_CLIENT "Support SASL authentication in the client library" OFF)

if (NOT (JS_ENGINE STREQUAL v8 OR
         JS_ENGINE STREQUAL sm))
  message(FATAL_ERROR "invalid JS_ENGINE ${JS_ENGINE}, options are sm or v8")
endif ()

set(CMAKE_MODULE_PATH
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/ft-index/cmake_modules"
  )

set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS
  MONGO_EXPOSE_MACROS
  SUPPORT_UTF8
  )

find_program(PYTHON2 NAMES python2.7 python2 python2.6 python)
if (PYTHON2 MATCHES NOTFOUND)
  message(SEND_ERROR "cannot find python 2")
endif ()

find_package(Threads)

include(CheckCXXCompilerFlag)
macro(set_cxxflags_if_supported)
  foreach(flag ${ARGN})
    check_cxx_compiler_flag(${flag} HAVE_CXX_${flag})
    if (HAVE_CXX_${flag})
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${flag}")
    endif ()
  endforeach(flag)
endmacro(set_cxxflags_if_supported)

set(CMAKE_C_FLAGS "-fPIC -fno-strict-aliasing -ggdb -Wall -Wsign-compare -Wno-unknown-pragmas -Winvalid-pch -Werror -pipe")
set(CMAKE_CXX_FLAGS "-fPIC -fno-strict-aliasing -ggdb")
set_cxxflags_if_supported(
  -Wall
  -Wsign-compare
  -Wno-unknown-pragmas
  -Winvalid-pch
  -Werror
  -pipe
  -Wnon-virtual-dtor
  -Woverloaded-virtual
  -Wno-unused-local-typedefs
  )
if (CMAKE_CXX_COMPILER_ID STREQUAL GNU)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-builtin-memcmp")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp")
endif ()
set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS
  _FILE_OFFSET_BITS=64
  )
set(CMAKE_C_FLAGS_DEBUG "-O0 -fstack-protector")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -fstack-protector")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "-O2 -g3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g3")
set(CMAKE_C_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS_DEBUG _DEBUG)

if (USE_SSL)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS MONGO_SSL)
  find_package(SSL REQUIRED)
  set(TOKUMX_SSL_LIBRARIES ${SSL_LIBRARIES})
else ()
  set(TOKUMX_SSL_LIBRARIES)
endif ()

include(CheckIncludeFiles)
check_include_files(limits.h MONGO_HAVE_HEADER_LIMITS_H)
if (MONGO_HAVE_HEADER_LIMITS_H)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS MONGO_HAVE_HEADER_LIMITS_H)
endif ()
check_include_files(unistd.h MONGO_HAVE_HEADER_UNISTD_H)
if (MONGO_HAVE_HEADER_UNISTD_H)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS MONGO_HAVE_HEADER_UNISTD_H)
endif ()
check_include_files(linux/magic.h MONGO_HAVE_HEADER_LINUX_MAGIC_H)
if (MONGO_HAVE_HEADER_LINUX_MAGIC_H)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS MONGO_HAVE_HEADER_LINUX_MAGIC_H=1)
endif ()
check_include_files(sys/disklabel.h MONGO_HAVE_HEADER_SYS_DISKLABEL_H)
if (MONGO_HAVE_HEADER_SYS_DISKLABEL_H)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS MONGO_HAVE_HEADER_SYS_DISKLABEL_H=1)
endif ()
check_include_files(sys/mount.h MONGO_HAVE_HEADER_SYS_MOUNT_H)
if (MONGO_HAVE_HEADER_SYS_MOUNT_H)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS MONGO_HAVE_HEADER_SYS_MOUNT_H=1)
endif ()
check_include_files(sys/param.h MONGO_HAVE_HEADER_SYS_PARAM_H)
if (MONGO_HAVE_HEADER_SYS_PARAM_H)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS MONGO_HAVE_HEADER_SYS_PARAM_H=1)
endif ()
check_include_files(sys/resource.h MONGO_HAVE_HEADER_SYS_RESOURCE_H)
if (MONGO_HAVE_HEADER_SYS_RESOURCE_H)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS MONGO_HAVE_HEADER_SYS_RESOURCE_H=1)
endif ()
check_include_files(sys/statfs.h MONGO_HAVE_HEADER_SYS_STATFS_H)
if (MONGO_HAVE_HEADER_SYS_STATFS_H)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS MONGO_HAVE_HEADER_SYS_STATFS_H=1)
endif ()
check_include_files(sys/vfs.h MONGO_HAVE_HEADER_SYS_VFS_H)
if (MONGO_HAVE_HEADER_SYS_VFS_H)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS MONGO_HAVE_HEADER_SYS_VFS_H=1)
endif ()
check_include_files(xfs/xfs.h MONGO_HAVE_HEADER_XFS_XFS_H)
if (MONGO_HAVE_HEADER_XFS_XFS_H)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS MONGO_HAVE_HEADER_XFS_XFS_H=1)
endif ()

if (USE_SASL_CLIENT)
  find_package(SASL REQUIRED)
endif ()

include(CheckSymbolExists)
check_symbol_exists(clock_gettime time.h HAVE_CLOCK_GETTIME)
if (HAVE_CLOCK_GETTIME)
  find_package(RT REQUIRED)
endif ()

check_include_files(execinfo.h MONGO_HAVE_EXECINFO_H)
if (MONGO_HAVE_EXECINFO_H)
  check_symbol_exists(backtrace execinfo.h MONGO_HAVE_BACKTRACE)
  check_symbol_exists(backtrace_symbols execinfo.h MONGO_HAVE_BACKTRACE_SYMBOLS)
  check_symbol_exists(backtrace_symbols_fd execinfo.h MONGO_HAVE_BACKTRACE_SYMBOLS_FD)
  if (MONGO_HAVE_BACKTRACE AND
      MONGO_HAVE_BACKTRACE_SYMBOLS AND
      MONGO_HAVE_BACKTRACE_SYMBOLS_FD)
    set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS MONGO_HAVE_EXECINFO_BACKTRACE)
  endif ()
endif ()

find_package(PCAP)
if (PCAP_FOUND)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS _HAVEPCAP)
endif ()

option(BUILD_TESTING "build tests" ON)
if (BUILD_TESTING)
  include(CTest)
endif ()

if (APPLE)
  set(CPACK_SYSTEM_NAME osx)
elseif (CMAKE_SYSTEM_NAME STREQUAL Linux)
  set(CPACK_SYSTEM_NAME linux)
elseif (CMAKE_SYSTEM_NAME STREQUAL FreeBSD)
  set(CPACK_SYSTEM_NAME freebsd)
elseif (CMAKE_SYSTEM_NAME STREQUAL SunOS)
  set(CPACK_SYSTEM_NAME solaris)
endif ()
if (CMAKE_SIZEOF_VOID_P EQUAL 8)
  set(CPACK_SYSTEM_NAME "${CPACK_SYSTEM_NAME}-x86_64")
else ()
  message(SEND_ERROR "tokumx isn't supported on 32-bit")
  set(CPACK_SYSTEM_NAME "${CPACK_SYSTEM_NAME}-x86")
endif ()
if (CMAKE_BUILD_TYPE STREQUAL Debug)
  set(CPACK_SYSTEM_NAME "${CPACK_SYSTEM_NAME}-debug")
endif ()

option(TOKUMX_STRIP_BINARIES "strip binaries before installing" TRUE)
if (TOKUMX_STRIP_BINARIES)
  find_program(OBJCOPY objcopy)
endif ()
function (install_executables component)
  foreach (target ${ARGN})
    if (TOKUMX_STRIP_BINARIES AND NOT OBJCOPY MATCHES NOTFOUND)
      add_custom_command(TARGET ${target} POST_BUILD
        COMMAND ${OBJCOPY} --only-keep-debug
                $<TARGET_FILE:${target}> $<TARGET_FILE_DIR:${target}>/${target}.dbg
        COMMAND ${OBJCOPY} --strip-debug
                $<TARGET_FILE:${target}>
        COMMAND ${OBJCOPY} --add-gnu-debuglink=${target}.dbg
                $<TARGET_FILE:${target}>
        )
      install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/${target}.dbg"
        COMPONENT tokumx_debuginfo
        DESTINATION bin
        )
      set_property(DIRECTORY APPEND PROPERTY ADDITIONAL_MAKE_CLEAN_FILES "${CMAKE_CURRENT_BINARY_DIR}/${target}.dbg")
    endif ()
    install(TARGETS
      ${target}
      COMPONENT ${component}
      DESTINATION bin
      )
  endforeach ()
endfunction ()

function(target_link_whole_libraries target)
  if (APPLE)
    foreach (lib ${ARGN})
      target_link_libraries(${target} -Wl,-force_load ${lib})
    endforeach ()
  else ()
    target_link_libraries(${target} -Wl,--start-group -Wl,-whole-archive ${ARGN} -Wl,-no-whole-archive -Wl,--end-group)
  endif ()
endfunction()

set(TOKU_CMAKE_SCRIPT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/ft-index/cmake")
include(TokuMergeLibs)

function(link_recursive_deps bin)
  set(_all_libs)
  set(_next_is_combined_libname 0)
  set(_combined_libname all_${bin}_deps)
  foreach (arg ${ARGN})
    if (_next_is_combined_libname)
      set(_combined_libname ${arg})
      set(_next_is_combined_libname 0)
    elseif (arg STREQUAL COMBINED_LIBNAME)
      set(_next_is_combined_libname 1)
    else ()
      list(APPEND _all_libs ${arg})
    endif ()
  endforeach ()
  list(LENGTH _all_libs _cur_num_libs)
  set(_last_num_libs 0)
  while (_cur_num_libs GREATER _last_num_libs)
    set(_new_libs)
    foreach (_lib ${_all_libs})
      get_target_property(_loc ${_lib} LOCATION)
      if (_loc)
        if (APPLE)
          # for unknown reasons, LINK_INTERFACE_LIBRARIES doesn't work on osx
          get_property(_libdeps
            TARGET ${_lib}
            PROPERTY INTERFACE_LINK_LIBRARIES
            )
        else ()
          get_property(_libdeps
            TARGET ${_lib}
            PROPERTY LINK_INTERFACE_LIBRARIES
            )
        endif ()
        list(APPEND _new_libs ${_libdeps})
      endif ()
    endforeach ()
    list(APPEND _all_libs ${_new_libs})
    list(REMOVE_DUPLICATES _all_libs)
    set(_last_num_libs ${_cur_num_libs})
    list(LENGTH _all_libs _cur_num_libs)
  endwhile ()
  if (NOT TARGET ${_combined_libname})
    toku_merge_static_libs(${_combined_libname} ${_combined_libname} "${_all_libs}")
  endif ()
  target_link_whole_libraries(${bin} ${_combined_libname})
endfunction()

function (remove_as_needed)
  if (UNIX AND NOT APPLE)
    foreach (arg ${ARGV})
      get_target_property(_as_needed_${arg}_link_flags ${arg} LINK_FLAGS)
      if (NOT _as_needed_${arg}_link_flags)
        set(_as_needed_${arg}_link_flags)
      endif ()
      set_target_properties(${arg} PROPERTIES LINK_FLAGS "${_as_needed_${arg}_link_flags} -Wl,--no-as-needed")
    endforeach ()
  endif ()
endfunction ()

if (CMAKE_SIZEOF_VOID_P EQUAL 8)
  set(INSTALL_LIBDIR lib64 CACHE STRING "where we should install libs")
else ()
  set(INSTALL_LIBDIR lib CACHE STRING "where we should install libs")
endif ()
option(TOKUMX_SET_RPATH "modify RPATH for self-contained package" TRUE)
if (TOKUMX_SET_RPATH)
  if (APPLE)
    set(CMAKE_INSTALL_RPATH
      "@loader_path/../${INSTALL_LIBDIR}"
      "@loader_path/../${INSTALL_LIBDIR}/plugins"
      )
  elseif (UNIX)
    set(CMAKE_INSTALL_RPATH
      "\$ORIGIN/../${INSTALL_LIBDIR}/"
      "\$\$ORIGIN/../${INSTALL_LIBDIR}/"
      "\$ORIGIN/../${INSTALL_LIBDIR}/plugins/"
      "\$\$ORIGIN/../${INSTALL_LIBDIR}/plugins/"
      )
  endif ()
endif ()

if (EXISTS "${CMAKE_SOURCE_DIR}/src/mongo/plugins/tokumx-backup-plugin/")
  set(TOKUMX_ENTERPRISE ON)
  add_definitions(-DMONGO_ENTERPRISE_VERSION=1)
else ()
  set(TOKUMX_ENTERPRISE OFF)
endif ()

function (git_version name dir)
  if ("${${name}_GIT_VERSION}" STREQUAL "")
    find_program(GIT git)
    if (GIT MATCHES NOTFOUND)
      set(FATAL_ERROR "git not found")
    endif ()

    get_filename_component(_real_dir ${dir} REALPATH)
    execute_process(
      COMMAND ${GIT} rev-parse HEAD
      WORKING_DIRECTORY "${_real_dir}"
      OUTPUT_VARIABLE _git_version
      RESULT_VARIABLE _git_retval
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET
      )
    if (NOT _git_retval EQUAL 0)
      message(WARNING "${GIT}: error: ${retval}")
      set(_git_version "unknown")
    endif ()
    set(${name}_GIT_VERSION ${_git_version} CACHE STRING "git version for ${name}")
  endif ()
  string(SUBSTRING ${${name}_GIT_VERSION} 0 7 _git_shortver)
  set(${name}_GIT_SHORTVER ${_git_shortver} CACHE STRING "git shortver for ${name}" FORCE)
endfunction ()

git_version(TOKUMX "${CMAKE_SOURCE_DIR}")
git_version(TOKUKV "${CMAKE_SOURCE_DIR}/src/third_party/ft-index")

if (TOKUMX_ENTERPRISE)
  git_version(TOKUBACKUP "${CMAKE_SOURCE_DIR}/src/third_party/backup/")
  git_version(TOKUMX_BACKUP_PLUGIN "${CMAKE_SOURCE_DIR}/src/mongo/plugins/tokumx-backup-plugin")
  git_version(TOKUMX_PITR_PLUGIN "${CMAKE_SOURCE_DIR}/src/mongo/plugins/tokumx-pitr-plugin")
endif ()

add_subdirectory(src/third_party)
include_directories(
  ${Boost_INCLUDE_DIRS}
  ${PCRE_INCLUDE_DIRS}
  ${S2_INCLUDE_DIRS}
  ${TokuKV_INCLUDE_DIRS}
  ${TokuBackup_INCLUDE_DIRS}
  )
if (JS_ENGINE STREQUAL v8)
  include_directories(${V8_INCLUDE_DIRS})
endif ()

add_subdirectory(src/mongo)

add_subdirectory(distsrc)

install(PROGRAMS scripts/tokumxstat.py
  COMPONENT tokumx_tools
  DESTINATION scripts
  )

install(PROGRAMS buildscripts/make_archive.py
  COMPONENT tokumx_client_misc
  DESTINATION buildscripts
  )
install(FILES src/SConscript.client
  COMPONENT tokumx_client_misc
  DESTINATION src
  )

add_subdirectory(jstests)

file(READ src/mongo/util/version.cpp version_cpp_contents)
string(REGEX MATCH "const char tokumxVersionString\\[\\] = \"(([0-9]+)\\.([0-9]+)\\.([0-9]+)([^\"]*))\"\\;" unused "${version_cpp_contents}")
set(CPACK_PACKAGE_VERSION ${CMAKE_MATCH_1})
set(CPACK_PACKAGE_VERSION_MAJOR ${CMAKE_MATCH_2})
set(CPACK_PACKAGE_VERSION_MINOR ${CMAKE_MATCH_3})
set(CPACK_PACKAGE_VERSION_PATCH ${CMAKE_MATCH_4})

if (TOKUMX_ENTERPRISE)
  set(TOKUMX_PRODUCTNAME "tokumx-e")
else ()
  set(TOKUMX_PRODUCTNAME "tokumx")
endif ()
if (NOT TOKUMX_DISTNAME)
  set(TOKUMX_DISTNAME "${TOKUMX_GIT_SHORTVER}-ft-${TOKUKV_GIT_SHORTVER}")
  if (TOKUMX_ENTERPRISE)
    set(TOKUMX_DISTNAME "${TOKUMX_DISTNAME}-ba-${TOKUBACKUP_GIT_SHORTVER}-bp-${TOKUMX_BACKUP_PLUGIN_GIT_SHORTVER}-pr-${TOKUMX_PITR_PLUGIN_GIT_SHORTVER}")
  endif ()
endif ()

set(CPACK_PACKAGE_FILE_NAME "${TOKUMX_PRODUCTNAME}-${TOKUMX_DISTNAME}-${CPACK_SYSTEM_NAME}")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "a high-performance mongodb server with fractal tree storage")
set(CPACK_PACKAGE_VENDOR "Tokutek, Inc.")
set(CPACK_ARCHIVE_COMPONENT_INSTALL TRUE)
set(CPACK_COMPONENT_INCLUDE_TOPLEVEL_DIRECTORY TRUE)
set(CPACK_GENERATOR TGZ)

set(CPACK_COMPONENT_TOKUMX_DEBUGINFO_GROUP debuginfo)
set(CPACK_COMPONENT_TOKUMX_SERVER_GROUP main)
set(CPACK_COMPONENT_TOKUMX_MISC_GROUP main)
set(CPACK_COMPONENT_TOKUMX_TOOLS_GROUP main)
set(CPACK_COMPONENT_TOKUMX_LIBS_EXTRA_GROUP main)
set(CPACK_COMPONENT_TOKUMX_PLUGINS_GROUP main)
set(CPACK_COMPONENT_TOKUMX_CLIENT_HEADERS_GROUP devel)
set(CPACK_COMPONENT_TOKUMX_CLIENT_SOURCES_GROUP devel-source)
set(CPACK_COMPONENT_TOKUMX_CLIENT_LIBS_GROUP devel)
set(CPACK_COMPONENT_TOKUMX_CLIENT_MISC_GROUP devel-source)
set(CPACK_COMPONENT_TOKUKV_LIBS_SHARED_GROUP main)
set(CPACK_COMPONENT_TOKUBACKUP_LIBS_SHARED_GROUP main)
set(CPACK_COMPONENTS_ALL
  tokumx_server
  tokumx_misc
  tokumx_tools
  tokumx_libs_extra
  tokumx_plugins
  tokumx_client_headers
  tokumx_client_libs
  tokukv_libs_shared
  tokubackup_libs_shared
  tokumx_debuginfo
  )

if (TOKUMX_ENTERPRISE)
  set(CPACK_COMPONENT_TOKUMX_AUDIT_LIBS_SHARED_GROUP main)
  list(APPEND CPACK_COMPONENTS_ALL tokumx_audit_libs_shared)
endif()

if (TOKUMX_ENTERPRISE)
  message(STATUS "Building TokuMX Enterprise ${CPACK_PACKAGE_VERSION}")
else ()
  message(STATUS "Building TokuMX Community ${CPACK_PACKAGE_VERSION}")
endif ()
message(STATUS "Package name: ${CPACK_PACKAGE_FILE_NAME}")

include(CPack)
