add_library(murmurhash3 STATIC MurmurHash3)

install(FILES MurmurHash3.h
  DESTINATION src/third_party/murmurhash3
  COMPONENT tokumx_client_sources
  )