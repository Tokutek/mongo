get_property(compile_defs DIRECTORY
  PROPERTY COMPILE_DEFINITIONS
  )
list(REMOVE_ITEM compile_defs MONGO_EXPOSE_MACROS)
set_property(DIRECTORY PROPERTY COMPILE_DEFINITIONS ${compile_defs})

set(normal_bins
  authTest
  clientTest
  httpClientTest
  tutorial
  whereExample
  )
set(abnormal_srcs
  first
  rs
  second
  )
set(abnormal_bins
  firstExample
  rsExample
  secondExample
  )

foreach (bin ${normal_bins})
  add_executable(${bin} ${bin})
endforeach ()
foreach (src ${abnormal_srcs})
  add_executable(${src}Example ${src})
endforeach ()

add_executable(bsondemo ../../bson/bsondemo/bsondemo)

foreach (bin ${normal_bins} ${abnormal_bins} bsondemo)
  add_dependencies(${bin} generate_error_codes generate_action_types)
  link_recursive_deps(${bin}
    COMBINED_LIBNAME client_example_deps
    mongoclient
    ${TOKUMX_SSL_LIBRARIES}
    )
endforeach ()

foreach (base ${normal_bins} ${abnormal_srcs})
  install(FILES ${base}.cpp
    DESTINATION src/mongo/client/examples
    COMPONENT tokumx_client_sources
    )
endforeach ()
