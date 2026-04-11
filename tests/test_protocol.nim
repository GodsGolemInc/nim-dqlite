import std/unittest
import dqlite/protocol

suite "dqlite wire protocol encoding":
  test "Given protocol version When encodeHandshake Then produces correct 8 bytes":
    let handshake = encodeHandshake()
    check handshake.len == 8
    check decodeUint64LE(handshake, 0) == DqliteProtocolVersion

  test "Given uint32 When encodeUint32LE Then little-endian bytes":
    var buf = newSeq[byte]()
    encodeUint32LE(buf, 0x04030201'u32)
    check buf == @[1'u8, 2, 3, 4]

  test "Given uint64 When encodeUint64LE Then little-endian bytes":
    var buf = newSeq[byte]()
    encodeUint64LE(buf, 42'u64)
    check buf.len == 8
    check decodeUint64LE(buf, 0) == 42'u64

  test "Given text 'test' When encodeText Then NUL-terminated and padded to 8":
    var buf = newSeq[byte]()
    encodeText(buf, "test")
    # "test" + NUL = 5 bytes, padded to 8
    check buf.len == 8
    check buf[0] == byte('t')
    check buf[3] == byte('t')
    check buf[4] == 0  # NUL
    check buf[5] == 0  # padding
    check buf[6] == 0
    check buf[7] == 0

  test "Given text length 8 When encodeText Then padded to 16":
    var buf = newSeq[byte]()
    encodeText(buf, "12345678")
    # 8 chars + NUL = 9 bytes, padded to 16
    check buf.len == 16

  test "Given empty text When encodeText Then 8 bytes of NUL+padding":
    var buf = newSeq[byte]()
    encodeText(buf, "")
    check buf.len == 8
    check buf[0] == 0  # NUL

  test "Given header When encodeHeader Then correct layout":
    let header = encodeHeader(reqExecSQL, 24, 0)
    check header.len == 8
    let decoded = decodeHeader(header)
    check decoded.typeCode == reqExecSQL
    check decoded.bodyWords == 3  # 24 / 8 = 3
    check decoded.schemaRevision == 0

  test "Given RequestClient(42) When encode Then header type=1 and body=42":
    let msg = encodeRequestClient(42)
    check msg.len == 16  # 8 header + 8 body
    let header = decodeHeader(msg)
    check header.typeCode == reqClient
    check header.bodyWords == 1
    check decodeUint64LE(msg, 8) == 42'u64

  test "Given RequestOpen('db') When encode Then contains db name":
    let msg = encodeRequestOpen("db")
    let header = decodeHeader(msg)
    check header.typeCode == reqOpen
    # Body: text("db") + uint64(flags=0) + text("")
    let (name, _) = decodeText(msg, 8)
    check name == "db"

  test "Given RequestLeader When encode Then empty body":
    let msg = encodeRequestLeader()
    check msg.len == 8
    let header = decodeHeader(msg)
    check header.typeCode == reqLeader
    check header.bodyWords == 0

  test "Given RequestExecSQL with no params When encode Then correct structure":
    let msg = encodeRequestExecSQL(1, "INSERT INTO t VALUES (1)")
    let header = decodeHeader(msg)
    check header.typeCode == reqExecSQL
    # Body: uint64(db_id) + text(sql) + params_tuple(empty)
    check decodeUint64LE(msg, 8) == 1'u64
    let (sql, _) = decodeText(msg, 16)
    check sql == "INSERT INTO t VALUES (1)"

  test "Given RequestExecSQL with 2 text params When encode Then param nibbles correct":
    let params = @[
      DqliteValue(kind: dvkText, textVal: "hello"),
      DqliteValue(kind: dvkText, textVal: "world"),
    ]
    let msg = encodeRequestExecSQL(0, "INSERT INTO t VALUES (?, ?)", params)
    let header = decodeHeader(msg)
    check header.typeCode == reqExecSQL

  test "Given RequestQuerySQL When encode Then type 9":
    let msg = encodeRequestQuerySQL(0, "SELECT * FROM t")
    let header = decodeHeader(msg)
    check header.typeCode == reqQuerySQL

suite "dqlite wire protocol decoding":
  test "Given ResponseWelcome bytes When decode Then returns heartbeat timeout":
    var data = newSeq[byte]()
    encodeUint64LE(data, 15000'u64)  # 15s heartbeat
    let timeout = decodeResponseWelcome(data)
    check timeout == 15000

  test "Given ResponseDb bytes When decode Then returns db_id":
    var data = newSeq[byte]()
    encodeUint32LE(data, 7'u32)
    encodeUint32LE(data, 0'u32)  # padding
    let dbId = decodeResponseDb(data)
    check dbId == 7

  test "Given ResponseNode bytes When decode Then returns id and address":
    var data = newSeq[byte]()
    encodeUint64LE(data, 123'u64)
    encodeText(data, "192.168.1.1:9001")
    let node = decodeResponseNode(data)
    check node.id == 123
    check node.address == "192.168.1.1:9001"

  test "Given ResponseResult bytes When decode Then returns lastInsertId and rowsAffected":
    var data = newSeq[byte]()
    encodeUint64LE(data, 42'u64)   # lastInsertId
    encodeUint64LE(data, 5'u64)    # rowsAffected
    let result = decodeResponseResult(data)
    check result.lastInsertId == 42
    check result.rowsAffected == 5

  test "Given ResponseFailure bytes When decode Then returns code and message":
    var data = newSeq[byte]()
    encodeUint64LE(data, ErrNotLeader)
    encodeText(data, "not leader")
    let (code, msg) = decodeResponseFailure(data)
    check code == ErrNotLeader
    check msg == "not leader"

  test "Given ResponseRows with 2 text columns 1 row When decode Then correct":
    var data = newSeq[byte]()
    # Column count
    encodeUint64LE(data, 2)
    # Column names
    encodeText(data, "id")
    encodeText(data, "name")
    # Row 1 type header: 2 columns, both text (type 3)
    # Nibble packing: byte = (col1_type) | (col2_type << 4) = 3 | (3 << 4) = 0x33
    var typeHeader = newSeq[byte]()
    typeHeader.add(0x33)
    padTo8(typeHeader)
    data.add(typeHeader)
    # Row 1 values
    encodeText(data, "1")
    encodeText(data, "Alice")

    let result = decodeResponseRows(data)
    check result.columns == @["id", "name"]
    check result.rows.len == 1
    check result.rows[0][0].textVal == "1"
    check result.rows[0][1].textVal == "Alice"

  test "Given ResponseRows with int64 and null When decode Then types parsed":
    var data = newSeq[byte]()
    encodeUint64LE(data, 2)  # 2 columns
    encodeText(data, "count")
    encodeText(data, "extra")
    # Type header: int64 (1) | null (5 << 4) = 0x51
    var typeHeader = newSeq[byte]()
    typeHeader.add(0x51)
    padTo8(typeHeader)
    data.add(typeHeader)
    # Values
    encodeInt64LE(data, 42)
    encodeUint64LE(data, 0)  # null placeholder

    let result = decodeResponseRows(data)
    check result.rows.len == 1
    check result.rows[0][0].kind == dvkInt64
    check result.rows[0][0].intVal == 42
    check result.rows[0][1].kind == dvkNull

  test "Given empty ResponseRows When decode Then empty result":
    var data = newSeq[byte]()
    encodeUint64LE(data, 0)
    let result = decodeResponseRows(data)
    check result.columns.len == 0
    check result.rows.len == 0

suite "dqlite value conversion":
  test "Given DqliteValue variants When dqliteValueToString Then correct string":
    check dqliteValueToString(DqliteValue(kind: dvkInt64, intVal: 42)) == "42"
    check dqliteValueToString(DqliteValue(kind: dvkFloat64, floatVal: 3.14)) == "3.14"
    check dqliteValueToString(DqliteValue(kind: dvkText, textVal: "hello")) == "hello"
    check dqliteValueToString(DqliteValue(kind: dvkNull)) == ""
    check dqliteValueToString(DqliteValue(kind: dvkBoolean, boolVal: true)) == "1"
    check dqliteValueToString(DqliteValue(kind: dvkBoolean, boolVal: false)) == "0"

  test "Given newDqliteError When created Then has code and message":
    let err = newDqliteError(768, "not leader")
    check err.code == 768
    check err.msg == "not leader"

  test "Given decodeText roundtrip When various strings Then preserved":
    for s in @["hello", "", "日本語", "a", "12345678", "123456789"]:
      var buf = newSeq[byte]()
      encodeText(buf, s)
      let (decoded, consumed) = decodeText(buf, 0)
      check decoded == s
      check consumed == buf.len
