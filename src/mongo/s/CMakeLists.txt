add_library(s_base STATIC
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
target_link_libraries(s_base LINK_PUBLIC
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
target_link_libraries(s_upgrade LINK_PUBLIC
  s_base
  mongocommon
  )

add_library(s_metadata STATIC
  collection_manager
  metadata_loader
  )
add_dependencies(s_metadata generate_error_codes generate_action_types)
target_link_libraries(s_metadata LINK_PUBLIC
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
target_link_libraries(coreshard LINK_PUBLIC
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
  version_mongos
  mongos_persistence_stubs
  )
add_dependencies(mongoscore generate_error_codes generate_action_types)
target_link_libraries(mongoscore LINK_PUBLIC
  authmongos
  client_parallel
  )

add_library(mongos_options STATIC
  mongos_options
  )
add_dependencies(mongos_options generate_error_codes generate_action_types)
target_link_libraries(mongos_options LINK_PUBLIC
  mongoscore
  coreshard
  mongocommon
  coreserver
  coredb
  options_parser
  )
