add_subdirectory(auth)
if (TOKUMX_ENTERPRISE)
  add_subdirectory(audit)
endif()

add_library(mongohasher STATIC
  hasher
  )
add_dependencies(mongohasher generate_error_codes generate_action_types)

add_library(server_parameters STATIC
  server_parameters
  )
add_dependencies(server_parameters generate_error_codes generate_action_types)
target_link_libraries(server_parameters foundation bson)

add_library(index_set STATIC
  index_set
  )
add_dependencies(index_set generate_error_codes generate_action_types)

add_library(dbcmdline STATIC
  cmdline
  )
target_link_libraries(dbcmdline LINK_PUBLIC
  bson
  server_parameters
  )

add_library(db_common STATIC
  field_ref
  )
target_link_libraries(db_common LINK_PUBLIC
  bson
  foundation
  )

add_library(geometry STATIC
  geo/hash
  geo/shapes
  )
target_link_libraries(geometry LINK_PUBLIC bson)

add_library(geoparser STATIC
  geo/geoparser
  )
target_link_libraries(geoparser LINK_PUBLIC bson geometry s2)

add_library(geoquery STATIC
  geo/geoquery
  )
target_link_libraries(geoquery LINK_PUBLIC geometry s2) 

add_library(coredb STATIC
  audit.cpp
  client_transaction
  opsettings
  commands.cpp
  commands/authentication_commands
  commands/connection_status
  commands/fail_point_cmd
  commands/find_and_modify_common
  commands/hashcmd
  commands/isself
  commands/mr_common
  commands/rename_collection_common
  commands/parameters
  commands/server_status
  pipeline/pipeline
  dbcommands_generic
  dbpath
  dbwebserver
  keypattern
  key_generator
  index/2d_key_generator
  index/hashed_key_generator
  index/haystack_key_generator
  index/s2_key_generator
  matcher
  spillable_vector
  txn_context
  gtid
  pipeline/accumulator
  pipeline/accumulator_add_to_set
  pipeline/accumulator_avg
  pipeline/accumulator_first
  pipeline/accumulator_last
  pipeline/accumulator_min_max
  pipeline/accumulator_push
  pipeline/accumulator_single_value
  pipeline/accumulator_sum
  pipeline/builder
  pipeline/doc_mem_monitor
  pipeline/document
  pipeline/document_source
  pipeline/document_source_bson_array
  pipeline/document_source_command_shards
  pipeline/document_source_filter_base
  pipeline/document_source_geo_near
  pipeline/document_source_group
  pipeline/document_source_limit
  pipeline/document_source_match
  pipeline/document_source_out
  pipeline/document_source_project
  pipeline/document_source_skip
  pipeline/document_source_sort
  pipeline/document_source_unwind
  pipeline/expression
  pipeline/expression_context
  pipeline/field_path
  pipeline/value
  projection
  querypattern
  queryutil
  stats/timer_stats
  stats/top
  descriptor
  index/2d_descriptor
  index/hashed_descriptor
  index/haystack_descriptor
  index/s2_descriptor
  storage/cursor
  storage/txn
  storage/env
  storage/key
  ../s/shardconnection
  )
add_dependencies(coredb generate_error_codes generate_action_types install_tdb_h)
target_link_libraries(coredb LINK_PUBLIC
  serverauth
  db_common
  mongocommon
  dbcmdline
  coreserver
  plugins
  server_parameters
  geoparser
  geoquery
  foundation
  ${TokuKV_LIBRARIES}
  )

if (TOKUMX_ENTERPRISE)
  target_link_libraries(coredb LINK_PUBLIC audit_enterprise)
endif()

add_library(serveronly STATIC
  curop
  kill_current_op
  interrupt_status_mongod
  crash
  d_globals
  ttl
  d_concurrency
  lockstat
  lockstate
  introspect
  clientcursor
  repl.cpp
  oplogreader
  repl/rs
  repl/consensus
  repl/rs_initiate
  repl/replset_commands
  repl/manager
  repl/health
  repl/heartbeat
  repl/rs_config
  repl/rs_sync
  repl/rs_initialsync
  repl/bgsync
  repl/rs_rollback
  oplog
  oplog_helpers
  repl_block
  indexcursor
  cloner
  indexer
  collection
  collection_map
  txn_complete_hooks
  matcher_covered
  dbeval
  restapi
  instance
  client
  client_load
  database
  cursor
  query_optimizer
  query_optimizer_internal
  queryoptimizercursorimpl
  query_plan
  query_plan_selection_policy
  parsed_query
  index/index
  index/hashed
  index/haystack
  index/partitioned
  index/s2
  index/2d
  scanandorder
  explain
  ops/count
  ops/delete
  ops/query
  ops/update
  ops/update_internal
  ops/insert
  geo/2d.cpp
  geo/geonear
  geo/haystack
  geo/s2common
  geo/s2cursor
  geo/s2nearcursor

  # most commands are only for mongod
  dbcommands
  command_cursors
  commands/dbcommands_deprecated
  commands/fsync
  commands/distinct
  commands/find_and_modify
  commands/group
  commands/mr
  commands/pipeline_command
  commands/txn_commands
  commands/load
  commands/testhooks
  pipeline/pipeline_d
  pipeline/document_source_cursor

  # Most storage/ files are in coredb, but this is server-only.
  storage/loader
  storage/indexer
  storage/dictionary
  
  ../util/elapsed_tracker
  stats/snapshots
  ../s/d_logic
  ../s/d_writeback
  ../s/d_migrate
  ../s/d_state
  ../s/d_split
  ../client/distlock_test
  ../s/d_chunk_manager
  module
  )
add_dependencies(serveronly generate_error_codes generate_action_types install_tdb_h)
target_link_libraries(serveronly LINK_PUBLIC
  client_parallel
  coreshard
  authmongod
  db_common
  dbcmdline
  defaultversion
  geoparser
  geoquery
  index_set
  ${TokuKV_LIBRARIES}
  )

add_library(mongodandmongos STATIC
  connection_factory
  initialize_server_global_state
  server_extra_log_context
  ../util/net/message_server_port
  )
add_dependencies(mongodandmongos generate_error_codes generate_action_types install_tdb_h)

add_library(coreserver STATIC
  client_basic
  common
  ../util/net/miniwebserver
  stats/counters
  stats/service_stats
  )
add_dependencies(coreserver generate_error_codes generate_action_types)
target_link_libraries(coreserver LINK_PUBLIC
  mongocommon
  scripting
  systeminfo
  )
