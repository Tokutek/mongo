find_path(YAML_INCLUDE_DIR yaml-cpp/yaml.h)
find_library(YAML_LIBRARY NAMES yaml)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(YAML DEFAULT_MSG
  YAML_LIBRARY YAML_INCLUDE_DIR)

if (YAML_FOUND)
  set(YAML_LIBRARIES ${YAML_LIBRARY})
  set(YAML_INCLUDE_DIRS ${YAML_INCLUDE_DIR})
else ()
  set(YAML_LIBRARIES)
  set(YAML_INCLUDE_DIRS)
endif ()

mark_as_advanced(YAML_LIBRARIES YAML_INCLUDE_DIRS)