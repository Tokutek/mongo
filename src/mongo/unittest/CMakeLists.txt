add_library(unittest STATIC
  unittest
  )
add_dependencies(unittest generate_error_codes generate_action_types)
target_link_libraries(unittest LINK_PUBLIC
  foundation
# TODO(sverch): 'lasterror' should not be required by 'foundation'.  Even if it is it should be
# depended on directly rather than here, but we currently have a triangle dependency between
# 'network', 'foundation' and 'lasterror'.  assert_util.cpp depends on lasterror.cpp which depends
# on sock.cpp which depends on assert_util.cpp.  Eventually we want to have an assertion library
# that does not depend on internal server code, and then this can go away.
  lasterror
  )

add_library(unittest_main STATIC
  unittest_main
  )
add_dependencies(unittest_main generate_error_codes generate_action_types)
target_link_libraries(unittest_main LINK_PUBLIC
  unittest
  base
  )

add_library(unittest_crutch STATIC
  crutch
  )
add_dependencies(unittest_crutch generate_error_codes generate_action_types)
