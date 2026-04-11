## Low-level C FFI bindings for dqlite.
##
## These bind the dqlite server management API (libdqlite).
## For SQL operations, use the high-level `db` module which
## abstracts over SQLite (dev) and dqlite wire protocol (prod).

const
  DQLITE_OK* = 0
  DQLITE_ERROR* = 1
  DQLITE_MISUSE* = 2
  DQLITE_NOMEM* = 3

type
  DqliteNodeId* = culonglong
  DqliteServer* = ptr object
  DqliteNode* = ptr object

  DqliteConnectFunc* = proc(arg: pointer, address: cstring, fd: ptr cint): cint {.cdecl.}

  DqliteNodeInfo* {.bycopy.} = object
    id*: DqliteNodeId
    address*: cstring

when defined(useDqlite):
  {.pragma: dqliteImport, importc, dynlib: "libdqlite.so".}

  # Version
  proc dqlite_version_number*(): cint {.dqliteImport.}

  # Server API (recommended for new code)
  proc dqlite_server_create*(path: cstring, server: ptr DqliteServer): cint {.dqliteImport.}
  proc dqlite_server_set_address*(server: DqliteServer, address: cstring): cint {.dqliteImport.}
  proc dqlite_server_set_auto_bootstrap*(server: DqliteServer, on: bool): cint {.dqliteImport.}
  proc dqlite_server_set_auto_join*(server: DqliteServer, addrs: ptr cstring, n: cuint): cint {.dqliteImport.}
  proc dqlite_server_set_bind_address*(server: DqliteServer, address: cstring): cint {.dqliteImport.}
  proc dqlite_server_start*(server: DqliteServer): cint {.dqliteImport.}
  proc dqlite_server_get_id*(server: DqliteServer): DqliteNodeId {.dqliteImport.}
  proc dqlite_server_handover*(server: DqliteServer): cint {.dqliteImport.}
  proc dqlite_server_stop*(server: DqliteServer): cint {.dqliteImport.}
  proc dqlite_server_destroy*(server: DqliteServer) {.dqliteImport.}

  # Legacy Node API
  proc dqlite_node_create*(id: DqliteNodeId, address: cstring, dataDir: cstring,
                           n: ptr DqliteNode): cint {.dqliteImport.}
  proc dqlite_node_destroy*(n: DqliteNode) {.dqliteImport.}
  proc dqlite_node_set_bind_address*(n: DqliteNode, address: cstring): cint {.dqliteImport.}
  proc dqlite_node_get_bind_address*(n: DqliteNode): cstring {.dqliteImport.}
  proc dqlite_node_set_network_latency_ms*(n: DqliteNode, ms: cuint): cint {.dqliteImport.}
  proc dqlite_node_start*(n: DqliteNode): cint {.dqliteImport.}
  proc dqlite_node_stop*(n: DqliteNode): cint {.dqliteImport.}
  proc dqlite_node_recover*(n: DqliteNode, infos: ptr DqliteNodeInfo,
                            nInfo: cint): cint {.dqliteImport.}
  proc dqlite_node_errmsg*(n: DqliteNode): cstring {.dqliteImport.}
  proc dqlite_generate_node_id*(address: cstring): DqliteNodeId {.dqliteImport.}
