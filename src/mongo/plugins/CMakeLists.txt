add_library(plugins STATIC
  commands
  dl
  loader
  )
add_dependencies(plugins generate_error_codes generate_action_types install_tdb_h)
target_link_libraries(plugins LINK_PUBLIC
  dl
  )

if (NOT TOKUMX_ENTERPRISE_USE_EXPORTS)
  file(GLOB dentries RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" *)
  foreach (dentry ${dentries})
    if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${dentry}/")
      add_subdirectory("${dentry}")
    endif ()
  endforeach ()
endif ()
