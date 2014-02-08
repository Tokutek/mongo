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

foreach (bin ${normal_bins} ${abnormal_bins})
  add_dependencies(${bin} generate_error_codes generate_action_types)
  target_link_libraries(${bin}
    mongoclient
    ${TOKUMX_SSL_LIBRARIES}
    )
endforeach ()

add_executable(bsondemo ../../bson/bsondemo/bsondemo)
add_dependencies(bsondemo generate_error_codes generate_action_types)
target_link_libraries(bsondemo
  mongoclient
  ${TOKUMX_SSL_LIBRARIES}
  )

foreach (base ${normal_bins} ${abnormal_srcs})
  install(FILES ${base}.cpp
    DESTINATION src/mongo/client/examples
    COMPONENT tokumx_client_sources
    )
endforeach ()