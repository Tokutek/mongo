find_library(PCAP_LIBRARY NAMES pcap wpcap)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PCAP DEFAULT_MSG
  PCAP_LIBRARY)

if (PCAP_FOUND)
  set(PCAP_LIBRARIES ${PCAP_LIBRARY})
else ()
  set(PCAP_LIBRARIES)
endif ()

mark_as_advanced(PCAP_LIBRARIES)