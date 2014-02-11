install(FILES
  NEWS
  README.md
  GNU-AGPL-3.0
  THIRD-PARTY-NOTICES
  DESTINATION .
  COMPONENT tokumx_misc
  )

install(FILES
  client/LICENSE.txt
  client/SConstruct
  DESTINATION .
  COMPONENT tokumx_client_misc
  )
  