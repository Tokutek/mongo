add_subdirectory(auth)
add_subdirectory(commands)
add_subdirectory(geo)
add_subdirectory(matcher)
add_subdirectory(ops)

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

add_library(lasterror STATIC
  lasterror
  )
add_dependencies(lasterror generate_error_codes generate_action_types)
target_link_libraries(lasterror LINK_PUBLIC
  network
  foundation
  )

# This library exists because some libraries, such as our networking library, need access to server
# options, but not to the helpers to set them from the command line.  libserver_options_core.a just
# has the structure for storing the server options, while libserver_options.a has the code to set
# them via the command line.
add_library(server_options_core STATIC
  server_options
  )
add_dependencies(server_options_core generate_error_codes generate_action_types)
target_link_libraries(server_options_core LINK_PUBLIC
  bson
  )

add_library(server_options STATIC
  server_options_helpers
  )
add_dependencies(server_options generate_error_codes generate_action_types)
target_link_libraries(server_options LINK_PUBLIC
  bson
  network  # temporary crutch that should go away once the networking library has separate options
  server_options_core
  server_parameters
  options_parser_init
  cmdline_utils
  )

add_library(mongod_options STATIC
  mongod_options
  )
add_dependencies(mongod_options generate_error_codes generate_action_types)
target_link_libraries(mongod_options LINK_PUBLIC
  server_options
  mongocommon
  serveronly
  coreserver
  coredb
  options_parser_init
  )

add_library(db_common STATIC
  field_ref
  field_parser
  )
target_link_libraries(db_common LINK_PUBLIC
  bson
  foundation
  )

add_library(storage_global_params_impl STATIC
  storage_global_params_impl
  )

add_library(coredb STATIC
  audit
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
  commands/server_status
  commands/shutdown
  commands/parameters
  commands/user_management_commands.cpp
  pipeline/pipeline
  dbcommands_generic
  dbwebserver
  keypattern
  keygenerator
  matcher.cpp
  matcher/matcher
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
  storage/cursor
  storage/txn
  storage/env
  storage/key
  ../s/shardconnection
  )
add_dependencies(coredb generate_error_codes generate_action_types install_tdb_h)
target_link_libraries(coredb LINK_PUBLIC
  serverauth
  usercommandsparser
  db_common
  mongocommon
  coreserver
  plugins
  server_parameters
  geoparser
  geoquery
  expressions
  expressions_geo
  expressions_where
  foundation
  server_options
  storage_global_params_impl
  cmdline_utils
  clientdriver
  ${TokuKV_LIBRARIES}
  )

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
  repl/replication_server_status
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
  index
  scanandorder
  explain
  # geo/geonear
  # geo/haystack
  # geo/s2common
  ops/count
  ops/delete
  ops/query
  ops/update
  ops/update_internal
  ops/insert
  startup_warnings
  storage_options

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
  storage_global_params_impl
  defaultversion
  index_set
  ${TokuKV_LIBRARIES}
  )

add_library(mongodandmongos STATIC
  initialize_server_global_state
  server_extra_log_context
  ../util/net/message_server_port
  )
add_dependencies(mongodandmongos generate_error_codes generate_action_types install_tdb_h)

add_library(coreserver STATIC
  client_basic
  ../util/net/miniwebserver
  stats/counters
  stats/service_stats
  log_process_details
  )
add_dependencies(coreserver generate_error_codes generate_action_types)
target_link_libraries(coreserver LINK_PUBLIC
  mongocommon
  scripting
  systeminfo
  )
