set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS
  ENABLE_DEBUGGER_SUPPORT
  )
set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS_DEBUG
  V8_ENABLE_CHECKS
  OBJECT_PRINT
  ENABLE_DISASSEMBLER
  DEBUG
  )

if (NOT UNIX)
  message(FATAL_ERROR "haven't written cmake for windows")
endif ()

set(V8_SOURCES
  src/accessors
  src/allocation
  src/api
  src/assembler
  src/ast
  src/atomicops_internals_x86_gcc
  src/bignum-dtoa
  src/bignum
  src/bootstrapper
  src/builtins
  src/cached-powers
  src/checks
  src/circular-queue
  src/code-stubs
  src/codegen
  src/compilation-cache
  src/compiler
  src/contexts
  src/conversions
  src/counters
  src/cpu-profiler
  src/data-flow
  src/date
  src/dateparser
  src/debug-agent
  src/debug
  src/deoptimizer
  src/disassembler
  src/diy-fp
  src/dtoa
  src/elements-kind
  src/elements
  src/execution
  src/extensions/externalize-string-extension
  src/extensions/gc-extension
  src/extensions/statistics-extension
  src/factory
  src/fast-dtoa
  src/fixed-dtoa
  src/flags
  src/frames
  src/full-codegen
  src/func-name-inferrer
  src/gdb-jit
  src/global-handles
  src/handles
  src/heap-profiler
  src/heap
  src/hydrogen-instructions
  src/hydrogen
  src/ic
  src/incremental-marking
  src/inspector
  src/interface
  src/interpreter-irregexp
  src/isolate
  src/jsregexp
  src/lithium-allocator
  src/lithium
  src/liveedit
  src/liveobjectlist
  src/log-utils
  src/log
  src/mark-compact
  src/messages
  src/objects-printer
  src/objects-visiting
  src/objects
  src/once
  src/optimizing-compiler-thread
  src/parser
  src/preparse-data
  src/preparser
  src/profile-generator
  src/property
  src/regexp-macro-assembler-irregexp
  src/regexp-macro-assembler
  src/regexp-stack
  src/rewriter
  src/runtime-profiler
  src/runtime
  src/safepoint-table
  src/scanner-character-streams
  src/scanner
  src/scopeinfo
  src/scopes
  src/serialize
  src/snapshot-common
  src/spaces
  src/store-buffer
  src/string-search
  src/string-stream
  src/strtod
  src/stub-cache
  src/token
  src/transitions
  src/type-info
  src/unicode
  src/utils
  src/v8-counters
  src/v8
  src/v8conversions
  src/v8threads
  src/v8utils
  src/variables
  src/version
  src/zone
  )

list(APPEND V8_SOURCES src/platform-posix)

set(V8_CXX_FLAGS
  -Wall
  -Werror
  -W
  -Wno-unused-parameter
  -Woverloaded-virtual
  -Wnon-virtual-dtor
  -Wno-unused-local-typedefs
  )
if (CMAKE_SYSTEM_NAME STREQUAL Linux)
  list(APPEND V8_CXX_FLAGS
    -ansi
    -pedantic
    )
  list(APPEND V8_SOURCES src/platform-linux)
elseif (CMAKE_SYSTEM_NAME STREQUAL Darwin)
  list(APPEND V8_CXX_FLAGS
    -ansi
    -pedantic
    )
  list(APPEND V8_SOURCES src/platform-macos)
elseif (CMAKE_SYSTEM_NAME STREQUAL FreeBSD)
  list(APPEND V8_CXX_FLAGS
    -ansi
    )
  list(APPEND V8_SOURCES src/platform-freebsd)
elseif (CMAKE_SYSTEM_NAME STREQUAL SunOS)
  list(APPEND V8_CXX_FLAGS
    -ansi
    )
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS
    __C99FEATURES__
    )
  list(APPEND V8_SOURCES src/platform-solaris)
endif ()

if (CMAKE_CXX_COMPILER_ID STREQUAL Clang)
  list(APPEND V8_CXX_FLAGS
    -Wno-tautological-constant-out-of-range-compare
    -Wno-unused-private-field
    )
endif ()

set_cxxflags_if_supported(${V8_CXX_FLAGS})

# 64 bit
if (CMAKE_SIZEOF_VOID_P EQUAL 8)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS
    V8_TARGET_ARCH_X64
    )
  list(APPEND V8_SOURCES
    src/x64/assembler-x64
    src/x64/builtins-x64
    src/x64/code-stubs-x64
    src/x64/codegen-x64
    src/x64/cpu-x64
    src/x64/debug-x64
    src/x64/deoptimizer-x64
    src/x64/disasm-x64
    src/x64/frames-x64
    src/x64/full-codegen-x64
    src/x64/ic-x64
    src/x64/lithium-codegen-x64
    src/x64/lithium-gap-resolver-x64
    src/x64/lithium-x64
    src/x64/macro-assembler-x64
    src/x64/regexp-macro-assembler-x64
    src/x64/stub-cache-x64
    )
else ()
  set_property(DIRECTORY APPEND COMPILE_DEFINITIONS
    V8_TARGET_ARCH_IA32
    )
  list(APPEND V8_SOURCES
    src/ia32/assembler-ia32
    src/ia32/builtins-ia32
    src/ia32/code-stubs-ia32
    src/ia32/codegen-ia32
    src/ia32/cpu-ia32
    src/ia32/debug-ia32
    src/ia32/deoptimizer-ia32
    src/ia32/disasm-ia32
    src/ia32/frames-ia32
    src/ia32/full-codegen-ia32
    src/ia32/ic-ia32
    src/ia32/lithium-codegen-ia32
    src/ia32/lithium-gap-resolver-ia32
    src/ia32/lithium-ia32
    src/ia32/macro-assembler-ia32
    src/ia32/regexp-macro-assembler-ia32
    src/ia32/stub-cache-ia32
    )
endif ()

if (CMAKE_BUILD_TYPE STREQUAL Debug)
  list(APPEND V8_SOURCES
    src/objects-debug
    src/prettyprinter
    src/regexp-macro-assembler-tracer
    )
endif ()

set(V8_EXPERIMENTAL_LIBRARY_FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/src/proxy.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/collection.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/macros.py"
  )

string(REGEX REPLACE ";" "," V8_EXPERIMENTAL_LIBRARY_FILES_LIST "${V8_EXPERIMENTAL_LIBRARY_FILES}")
add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/experimental-libraries.cc"
  COMMAND
    "${PYTHON2}"
        "${CMAKE_CURRENT_SOURCE_DIR}/js2c-wrap.py"
        "${CMAKE_CURRENT_SOURCE_DIR}/tools"
        "${CMAKE_CURRENT_BINARY_DIR}/experimental-libraries.cc"
        "${V8_EXPERIMENTAL_LIBRARY_FILES_LIST}"
        EXPERIMENTAL
        off
  DEPENDS ${V8_EXPERIMENTAL_LIBRARY_FILES}
  )
add_custom_target(generate_v8_experimental_libraries
  DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/experimental-libraries.cc"
  )

set(V8_LIBRARY_FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/src/runtime.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/v8natives.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/array.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/string.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/uri.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/math.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/messages.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/apinatives.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/date.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/regexp.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/json.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/liveedit-debugger.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/mirror-debugger.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/debug-debugger.js"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/macros.py"
  )

string(REGEX REPLACE ";" "," V8_LIBRARY_FILES_LIST "${V8_LIBRARY_FILES}")
add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/libraries.cc"
  COMMAND
    "${PYTHON2}"
        "${CMAKE_CURRENT_SOURCE_DIR}/js2c-wrap.py"
        "${CMAKE_CURRENT_SOURCE_DIR}/tools"
        "${CMAKE_CURRENT_BINARY_DIR}/libraries.cc"
        "${V8_LIBRARY_FILES_LIST}"
        CORE
        off
  DEPENDS ${V8_LIBRARY_FILES}
  )
add_custom_target(generate_v8_libraries
  DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/libraries.cc"
  )

list(APPEND V8_SOURCES
  "${CMAKE_CURRENT_BINARY_DIR}/libraries"
  "${CMAKE_CURRENT_BINARY_DIR}/experimental-libraries"
  src/snapshot-empty
  )

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src")
add_library(v8 STATIC ${V8_SOURCES})
add_dependencies(v8
  generate_v8_experimental_libraries
  generate_v8_libraries
  )

set(V8_FOUND TRUE CACHE BOOL "V8 found")
set(V8_INCLUDE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/include" CACHE STRING "V8 include dirs")
set(V8_LIBRARIES v8 CACHE STRING "V8 libraries")

mark_as_advanced(V8_FOUND V8_INCLUDE_DIRS V8_LIBRARIES)
