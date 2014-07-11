add_library(platform STATIC
  backtrace
  posix_fadvise
  process_id.cpp
  random
  strcasestr
  )
add_dependencies(platform generate_error_codes generate_action_types)
