add_library(thread_name STATIC
  thread_name
  )
add_dependencies(thread_name generate_error_codes generate_action_types install_tdb_h)
target_link_libraries(thread_name LINK_PUBLIC
  base
  ${Boost_LIBRARIES}
  )

add_library(spin_lock STATIC
  spin_lock
  )
add_dependencies(spin_lock generate_error_codes generate_action_types)
