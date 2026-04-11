import std/[unittest, strutils]
import dqlite/protocol
import dqlite/client

# --- Mock response builders ---

proc buildWelcomeResponse(heartbeatMs: uint64 = 15000): seq[byte] =
  ## Build a complete Welcome response (header + body).
  var body = newSeq[byte]()
  encodeUint64LE(body, heartbeatMs)
  result = encodeHeader(resWelcome, body.len)
  result.add(body)

proc buildDbResponse(dbId: uint32 = 0): seq[byte] =
  var body = newSeq[byte]()
  encodeUint32LE(body, dbId)
  encodeUint32LE(body, 0)  # padding
  result = encodeHeader(resDb, body.len)
  result.add(body)

proc buildNodeResponse(id: uint64, address: string): seq[byte] =
  var body = newSeq[byte]()
  encodeUint64LE(body, id)
  encodeText(body, address)
  result = encodeHeader(resNode, body.len)
  result.add(body)

proc buildResultResponse(lastInsertId: uint64 = 0, rowsAffected: uint64 = 0): seq[byte] =
  var body = newSeq[byte]()
  encodeUint64LE(body, lastInsertId)
  encodeUint64LE(body, rowsAffected)
  result = encodeHeader(resResult, body.len)
  result.add(body)

proc buildRowsResponse(columns: seq[string], rows: seq[seq[string]]): seq[byte] =
  ## Build a Rows response with text-only values.
  var body = newSeq[byte]()
  encodeUint64LE(body, uint64(columns.len))
  for col in columns:
    encodeText(body, col)
  for row in rows:
    # Type header: all text (type 3)
    var typeHeader = newSeq[byte]()
    for i in 0 ..< columns.len:
      let typeCode = 3'u8  # dvkText
      if i mod 2 == 0:
        typeHeader.add(typeCode)
      else:
        typeHeader[^1] = typeHeader[^1] or (typeCode shl 4)
    padTo8(typeHeader)
    body.add(typeHeader)
    for val in row:
      encodeText(body, val)
  result = encodeHeader(resRows, body.len)
  result.add(body)

proc buildFailureResponse(code: uint64, msg: string): seq[byte] =
  var body = newSeq[byte]()
  encodeUint64LE(body, code)
  encodeText(body, msg)
  result = encodeHeader(resFailure, body.len)
  result.add(body)

suite "dqlite client with mock transport":
  test "Given mock Welcome+Db responses When connectWithTransport Then returns valid conn":
    # Sequence: handshake(sent, no response) → RequestClient → Welcome → RequestOpen → Db
    let responses = @[
      buildWelcomeResponse(),          # response to RequestClient
      buildDbResponse(0),              # response to RequestOpen
    ]
    let transport = newMockTransport(responses)
    let conn = connectWithTransport(transport, "test.db")
    check conn.connected == true
    check conn.dbId == 0
    check conn.dbName == "test.db"
    conn.close()
    check conn.connected == false

  test "Given mock ResponseResult When execSQL Then returns affected rows":
    let responses = @[
      buildWelcomeResponse(),
      buildDbResponse(0),
      buildResultResponse(lastInsertId = 5, rowsAffected = 1),
    ]
    let transport = newMockTransport(responses)
    let conn = connectWithTransport(transport, "test.db")
    defer: conn.close()

    let result = conn.execSQL("INSERT INTO t (x) VALUES (?)",
                              @[DqliteValue(kind: dvkText, textVal: "hello")])
    check result.lastInsertId == 5
    check result.rowsAffected == 1

  test "Given mock ResponseRows When querySQL Then returns correct data":
    let responses = @[
      buildWelcomeResponse(),
      buildDbResponse(0),
      buildRowsResponse(
        @["id", "name"],
        @[@["1", "Alice"], @["2", "Bob"]],
      ),
    ]
    let transport = newMockTransport(responses)
    let conn = connectWithTransport(transport, "test.db")
    defer: conn.close()

    let result = conn.querySQL("SELECT id, name FROM users")
    check result.columns == @["id", "name"]
    check result.rows.len == 2
    check dqliteValueToString(result.rows[0][0]) == "1"
    check dqliteValueToString(result.rows[0][1]) == "Alice"
    check dqliteValueToString(result.rows[1][0]) == "2"
    check dqliteValueToString(result.rows[1][1]) == "Bob"

  test "Given disconnected conn When execSQL Then raises DqliteError":
    let responses = @[
      buildWelcomeResponse(),
      buildDbResponse(0),
    ]
    let transport = newMockTransport(responses)
    let conn = connectWithTransport(transport, "test.db")
    conn.close()

    expect(DqliteError):
      discard conn.execSQL("SELECT 1")

  test "Given no responses When connectWithTransport Then raises DqliteError":
    let transport = newMockTransport(@[])
    expect(DqliteError):
      discard connectWithTransport(transport, "test.db")

  test "Given empty query result When querySQL Then returns empty rows":
    let responses = @[
      buildWelcomeResponse(),
      buildDbResponse(0),
      buildRowsResponse(@["x"], @[]),
    ]
    let transport = newMockTransport(responses)
    let conn = connectWithTransport(transport, "test.db")
    defer: conn.close()

    let result = conn.querySQL("SELECT x FROM empty_table")
    check result.columns == @["x"]
    check result.rows.len == 0

  test "Given multiple execs When execSQL Then all succeed":
    let responses = @[
      buildWelcomeResponse(),
      buildDbResponse(0),
      buildResultResponse(rowsAffected = 0),  # CREATE TABLE
      buildResultResponse(lastInsertId = 1, rowsAffected = 1),  # INSERT 1
      buildResultResponse(lastInsertId = 2, rowsAffected = 1),  # INSERT 2
    ]
    let transport = newMockTransport(responses)
    let conn = connectWithTransport(transport, "test.db")
    defer: conn.close()

    discard conn.execSQL("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    let r1 = conn.execSQL("INSERT INTO t VALUES (1, 'a')")
    check r1.lastInsertId == 1
    let r2 = conn.execSQL("INSERT INTO t VALUES (2, 'b')")
    check r2.lastInsertId == 2

  test "Given failure response When execSQL Then raises DqliteError with code":
    let responses = @[
      buildWelcomeResponse(),
      buildDbResponse(0),
      buildFailureResponse(1, "table not found: t"),
    ]
    let transport = newMockTransport(responses)
    let conn = connectWithTransport(transport, "test.db")
    defer: conn.close()

    var caught = false
    try:
      discard conn.execSQL("INSERT INTO t VALUES (1)")
    except DqliteError as e:
      caught = true
      check e.code == 1
      check "table not found" in e.msg
    check caught

  test "Given failure response When querySQL Then raises DqliteError":
    let responses = @[
      buildWelcomeResponse(),
      buildDbResponse(0),
      buildFailureResponse(1, "no such table: missing"),
    ]
    let transport = newMockTransport(responses)
    let conn = connectWithTransport(transport, "test.db")
    defer: conn.close()

    expect(DqliteError):
      discard conn.querySQL("SELECT * FROM missing")

  test "Given NodeInfo response When buildNodeResponse Then correct encoding":
    let resp = buildNodeResponse(42, "10.0.0.1:9001")
    # Skip header (8 bytes), decode body
    let node = decodeResponseNode(resp[8 .. ^1])
    check node.id == 42
    check node.address == "10.0.0.1:9001"
