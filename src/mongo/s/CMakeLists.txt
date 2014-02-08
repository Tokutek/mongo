add_library(s_base STATIC
  field_parser
  mongo_version_range
  type_changelog
  type_chunk
  type_collection
  type_database
  type_locks
  type_lockpings
  type_settings
  type_shard
  type_config_version
  type_mongos
  type_tags
  )
add_dependencies(s_base generate_error_codes generate_action_types)
target_link_libraries(s_base LINK_PRIVATE
  base
  bson
  )

add_library(s_upgrade STATIC
  cluster_client_internal
  config_upgrade
  config_upgrade_helpers
  config_upgrade_v0_to_v4
  config_upgrade_v3_to_v4
  )
add_dependencies(s_upgrade generate_error_codes generate_action_types)
target_link_libraries(s_upgrade LINK_PRIVATE
  s_base
  mongocommon
  )

add_library(s_metadata STATIC
  collection_manager
  metadata_loader
  )
add_dependencies(s_metadata generate_error_codes generate_action_types)
target_link_libraries(s_metadata LINK_PRIVATE
  s_base
  clientandshell
  base
  bson
  )

add_library(defaultversion STATIC
  default_version
  )
add_dependencies(defaultversion generate_error_codes generate_action_types)

add_library(coreshard STATIC
  ../client/distlock
  config
  grid
  chunk
  shard
  shardkey
  )
add_dependencies(coreshard generate_error_codes generate_action_types)
target_link_libraries(coreshard LINK_PRIVATE
  s_base
  )

add_library(mongoscore STATIC
  interrupt_status_mongos
  strategy
  strategy_shard
  strategy_single
  commands_admin
  commands_public
  commands_deprecated
  request
  client_info
  config_server_checker_service
  cursors
  s_only
  balance
  balancer_policy
  writeback_listener
  version_manager
  )
add_dependencies(mongoscore generate_error_codes generate_action_types)
target_link_libraries(mongoscore LINK_PRIVATE
  authmongos
  client_parallel
  )

if (BUILD_TESTING)
  foreach (test
      chunk_version_test
      field_parser_test
      mongo_version_range_test
      type_changelog_test
      type_chunk_test
      type_collection_test
      type_config_version_test
      type_database_test
      type_locks_test
      type_lockpings_test
      type_settings_test
      type_shard_test
      type_mongos_test
      type_tags_test
      )
    add_executable(${test} ${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    target_link_whole_libraries(${test}
      s_base
      unittest_main
      foundation
      unittest_crutch
      base
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()

  target_link_libraries(mongo_version_range_test bson)

  # no crutch
  foreach (test
      balancer_policy_tests
      chunk_diff_test
      collection_manager_test
      metadata_loader_test
      shard_conn_test
      shard_test
      )
    add_executable(${test} ${test})
    add_dependencies(${test} generate_error_codes generate_action_types)
    target_link_libraries(${test}
      s_metadata
      unittest_main
      foundation
      )
    target_link_whole_libraries(${test}
      mongocommon
      base
      ${TOKUMX_SSL_LIBRARIES}
      )
  endforeach ()
  foreach (test
      balancer_policy_tests
      shard_conn_test
      shard_test
      )
    target_link_whole_libraries(${test}
      mongoscore
      authservercommon
      coreshard
      coreserver
      coredb
      dbcmdline
      mongodandmongos
      )
    target_link_libraries(${test}
      ${TokuKV_LIBRARIES}
      )
  endforeach ()
  target_link_libraries(collection_manager_test mocklib)
  target_link_libraries(metadata_loader_test mocklib)
  target_link_libraries(shard_conn_test mocklib)
endif ()
