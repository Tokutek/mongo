find_library(SSL_LIBRARY NAMES ssl)
find_library(CRYPTO_LIBRARY NAMES crypto)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SSL DEFAULT_MSG
  SSL_LIBRARY CRYPTO_LIBRARY)

if (SSL_FOUND)
  set(SSL_LIBRARIES ${SSL_LIBRARY} ${CRYPTO_LIBRARY})
else ()
  set(SSL_LIBRARIES)
endif ()

mark_as_advanced(SSL_LIBRARIES)