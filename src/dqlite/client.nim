## dqlite TCP client with leader discovery and failover.
##
## Connects to a dqlite cluster, discovers the leader node, and provides
## SQL execution over the wire protocol. Uses a Transport abstraction
## for testability (mock transport in tests, TCP in production).

import std/[net, strutils]
import ./protocol
export protocol.DqliteError, protocol.DqliteValue, protocol.DqliteValueKind,
       protocol.ExecResult, protocol.QueryResult, protocol.NodeInfo

type
  SendProc* = proc(data: openArray[byte]) {.raises: [DqliteError], gcsafe.}
  RecvProc* = proc(n: int): seq[byte] {.raises: [DqliteError], gcsafe.}

  Transport* = object
    sendProc*: SendProc
    recvProc*: RecvProc
    closeProc*: proc() {.raises: [], gcsafe.}

  DqliteConn* = ref object
    transport*: Transport
    dbId*: uint32
    addresses*: seq[string]
    currentAddr*: string
    dbName*: string
    connected*: bool
    heartbeatTimeout*: uint64  ## Server heartbeat timeout in ms

# --- Transport implementations ---

proc newTcpTransport*(address: string, timeoutMs: int = 5000): Transport {.raises: [DqliteError].} =
  ## Create a TCP transport to the given host:port address.
  let parts = address.split(":")
  if parts.len != 2:
    raise newDqliteError(0, "Invalid address format (expected host:port): " & address)

  let host = parts[0]
  let port: int =
    try:
      parseInt(parts[1])
    except ValueError:
      raise newDqliteError(0, "Invalid port: " & parts[1])

  var sock: Socket
  try:
    sock = newSocket()
    sock.connect(host, Port(port), timeoutMs)
  except CatchableError as e:
    raise newDqliteError(0, "TCP connect to " & address & " failed: " & e.msg)

  let sockRef = sock  # capture for closures

  result.sendProc = proc(data: openArray[byte]) {.raises: [DqliteError], gcsafe.} =
    try:
      let sent = sockRef.send(unsafeAddr data[0], data.len)
      if sent != data.len:
        raise newDqliteError(0, "Short send: " & $sent & "/" & $data.len)
    except OSError as e:
      raise newDqliteError(0, "Send failed: " & e.msg)

  result.recvProc = proc(n: int): seq[byte] {.raises: [DqliteError], gcsafe.} =
    result = newSeq[byte](n)
    var received = 0
    while received < n:
      let got =
        try:
          sockRef.recv(addr result[received], n - received)
        except OSError as e:
          raise newDqliteError(0, "Recv failed: " & e.msg)
      if got <= 0:
        raise newDqliteError(0, "Connection closed by peer")
      received += got

  result.closeProc = proc() {.raises: [], gcsafe.} =
    try:
      sockRef.close()
    except CatchableError:
      discard

proc newMockTransport*(responses: seq[seq[byte]]): Transport {.raises: [].} =
  ## Create a mock transport for testing. Returns pre-built responses
  ## in order. Sent data is discarded.
  var idx = 0
  var responsesCopy = responses
  var readPos = 0

  result.sendProc = proc(data: openArray[byte]) {.raises: [DqliteError], gcsafe.} =
    discard # ignore sent data

  result.recvProc = proc(n: int): seq[byte] {.raises: [DqliteError], gcsafe.} =
    result = newSeq[byte](n)
    var filled = 0
    while filled < n:
      if idx >= responsesCopy.len:
        raise newDqliteError(0, "Mock transport: no more responses")
      let available = responsesCopy[idx].len - readPos
      let toRead = min(n - filled, available)
      for i in 0 ..< toRead:
        result[filled + i] = responsesCopy[idx][readPos + i]
      filled += toRead
      readPos += toRead
      if readPos >= responsesCopy[idx].len:
        inc idx
        readPos = 0

  result.closeProc = proc() {.raises: [], gcsafe.} =
    discard

# --- Internal helpers ---

proc sendMsg(conn: DqliteConn, data: openArray[byte]) {.raises: [DqliteError].} =
  conn.transport.sendProc(data)

proc recvBytes(conn: DqliteConn, n: int): seq[byte] {.raises: [DqliteError].} =
  conn.transport.recvProc(n)

proc recvHeader(conn: DqliteConn): MsgHeader {.raises: [DqliteError].} =
  let headerBytes = conn.recvBytes(8)
  var arr: array[8, byte]
  for i in 0 ..< 8:
    arr[i] = headerBytes[i]
  result = decodeHeader(arr)

proc recvBody(conn: DqliteConn, header: MsgHeader): seq[byte] {.raises: [DqliteError].} =
  let bodySize = int(header.bodyWords) * 8
  if bodySize == 0:
    return @[]
  result = conn.recvBytes(bodySize)

proc recvResponse(conn: DqliteConn): (MsgHeader, seq[byte]) {.raises: [DqliteError].} =
  let header = conn.recvHeader()
  let body = conn.recvBody(header)

  # Check for failure response
  if header.typeCode == resFailure:
    let (code, msg) = decodeResponseFailure(body)
    raise newDqliteError(code, msg)

  result = (header, body)

proc handshake(conn: DqliteConn) {.raises: [DqliteError].} =
  ## Send protocol version handshake.
  let hs = encodeHandshake()
  conn.sendMsg(hs)

proc registerClient(conn: DqliteConn) {.raises: [DqliteError].} =
  ## Send RequestClient and receive Welcome.
  let msg = encodeRequestClient(0)
  conn.sendMsg(msg)
  let (header, body) = conn.recvResponse()
  if header.typeCode != resWelcome:
    raise newDqliteError(0, "Expected Welcome response, got type " & $header.typeCode)
  conn.heartbeatTimeout = decodeResponseWelcome(body)

proc openDb(conn: DqliteConn) {.raises: [DqliteError].} =
  ## Send RequestOpen and receive Db response.
  let msg = encodeRequestOpen(conn.dbName)
  conn.sendMsg(msg)
  let (header, body) = conn.recvResponse()
  if header.typeCode != resDb:
    raise newDqliteError(0, "Expected Db response, got type " & $header.typeCode)
  conn.dbId = decodeResponseDb(body)

proc findLeader(conn: DqliteConn): string {.raises: [DqliteError].} =
  ## Send RequestLeader and return leader address.
  let msg = encodeRequestLeader()
  conn.sendMsg(msg)
  let (header, body) = conn.recvResponse()
  if header.typeCode != resNode:
    raise newDqliteError(0, "Expected Node response, got type " & $header.typeCode)
  let node = decodeResponseNode(body)
  result = node.address

proc connectToAddress(conn: DqliteConn, address: string,
                      transportFactory: proc(a: string): Transport {.raises: [DqliteError].}) {.raises: [DqliteError].} =
  conn.transport = transportFactory(address)
  conn.currentAddr = address
  conn.handshake()
  conn.registerClient()

# --- Public API ---

proc connect*(addresses: seq[string], dbName: string = "db",
              transportFactory: proc(a: string): Transport {.raises: [DqliteError].} = nil): DqliteConn {.raises: [DqliteError].} =
  ## Connect to a dqlite cluster. Discovers the leader node automatically.
  ## Tries each address in sequence, follows leader redirects.
  ## Pass a custom transportFactory for testing (defaults to TCP).
  if addresses.len == 0:
    raise newDqliteError(0, "No addresses provided")

  let factory = if transportFactory.isNil:
    proc(address: string): Transport {.raises: [DqliteError].} = newTcpTransport(address)
  else:
    transportFactory

  result = DqliteConn(
    addresses: addresses,
    dbName: dbName,
    connected: false,
  )

  var lastErr = ""
  for address in addresses:
    try:
      result.connectToAddress(address, factory)

      # Check if this node is the leader
      let leaderAddr = result.findLeader()
      if leaderAddr != address and leaderAddr.len > 0:
        # Redirect to the actual leader
        result.transport.closeProc()
        result.connectToAddress(leaderAddr, factory)
        let verifiedLeader = result.findLeader()
        if verifiedLeader != leaderAddr and verifiedLeader.len > 0:
          raise newDqliteError(0, "Leader redirect loop: " & leaderAddr & " -> " & verifiedLeader)

      # Open the database
      result.openDb()
      result.connected = true
      return
    except DqliteError as e:
      lastErr = e.msg
      if result.transport.closeProc != nil:
        result.transport.closeProc()
      continue

  raise newDqliteError(0, "Failed to connect to any node: " & lastErr)

proc connectWithTransport*(transport: Transport, dbName: string = "db"): DqliteConn {.raises: [DqliteError].} =
  ## Connect using a pre-built transport (for testing with MockTransport).
  result = DqliteConn(
    transport: transport,
    dbName: dbName,
    addresses: @["mock"],
    currentAddr: "mock",
    connected: false,
  )
  result.handshake()
  result.registerClient()
  result.openDb()
  result.connected = true

proc execSQL*(conn: DqliteConn, sql: string,
              params: seq[DqliteValue] = @[]): ExecResult {.raises: [DqliteError].} =
  ## Execute a SQL statement (INSERT, UPDATE, DELETE, CREATE, etc.).
  if not conn.connected:
    raise newDqliteError(0, "Not connected")
  let msg = encodeRequestExecSQL(conn.dbId, sql, params)
  conn.sendMsg(msg)
  let (header, body) = conn.recvResponse()
  if header.typeCode != resResult:
    raise newDqliteError(0, "Expected Result response, got type " & $header.typeCode)
  result = decodeResponseResult(body)

proc querySQL*(conn: DqliteConn, sql: string,
               params: seq[DqliteValue] = @[]): QueryResult {.raises: [DqliteError].} =
  ## Execute a SQL query (SELECT) and return rows.
  if not conn.connected:
    raise newDqliteError(0, "Not connected")
  let msg = encodeRequestQuerySQL(conn.dbId, sql, params)
  conn.sendMsg(msg)
  let (header, body) = conn.recvResponse()
  if header.typeCode != resRows:
    raise newDqliteError(0, "Expected Rows response, got type " & $header.typeCode)
  result = decodeResponseRows(body)

proc close*(conn: DqliteConn) {.raises: [].} =
  ## Close the connection.
  if conn.connected:
    conn.transport.closeProc()
    conn.connected = false
