## dqlite wire protocol encoder/decoder.
##
## Pure module — no I/O, no side effects. All functions operate on seq[byte].
## Implements the dqlite binary protocol (8-byte word aligned, little-endian).
##
## Reference: https://dqlite.io/docs/reference/wire-protocol


const
  DqliteProtocolVersion* = 1'u64
  WordSize = 8

  # Request type codes
  reqLeader*    = 0'u8
  reqClient*    = 1'u8
  reqHeartbeat* = 2'u8
  reqOpen*      = 3'u8
  reqPrepare*   = 4'u8
  reqExec*      = 5'u8
  reqQuery*     = 6'u8
  reqFinalize*  = 7'u8
  reqExecSQL*   = 8'u8
  reqQuerySQL*  = 9'u8
  reqInterrupt* = 10'u8
  reqAdd*       = 12'u8
  reqAssign*    = 13'u8
  reqRemove*    = 14'u8
  reqDump*      = 15'u8
  reqCluster*   = 16'u8
  reqTransfer*  = 17'u8

  # Response type codes
  resFailure*   = 0'u8
  resNode*      = 1'u8
  resWelcome*   = 2'u8
  resDb*        = 3'u8
  resStmt*      = 4'u8
  resResult*    = 6'u8
  resRows*      = 7'u8

  # dqlite error code for leader redirect
  ErrNotLeader* = 768'u64

type
  DqliteError* = object of CatchableError
    code*: uint64

  DqliteValueKind* = enum
    dvkUnknown = 0
    dvkInt64 = 1
    dvkFloat64 = 2
    dvkText = 3
    dvkBlob = 4
    dvkNull = 5
    dvkISO8601 = 11
    dvkBoolean = 12

  DqliteValue* = object
    case kind*: DqliteValueKind
    of dvkUnknown: discard
    of dvkInt64: intVal*: int64
    of dvkFloat64: floatVal*: float64
    of dvkText, dvkISO8601: textVal*: string
    of dvkBlob: blobVal*: seq[byte]
    of dvkNull: discard
    of dvkBoolean: boolVal*: bool

  MsgHeader* = object
    bodyWords*: uint32
    typeCode*: uint8
    schemaRevision*: uint8

  ExecResult* = object
    lastInsertId*: uint64
    rowsAffected*: uint64

  QueryResult* = object
    columns*: seq[string]
    rows*: seq[seq[DqliteValue]]

  NodeInfo* = object
    id*: uint64
    address*: string

# --- Low-level encoding ---

proc padTo8*(buf: var seq[byte]) {.raises: [].} =
  let remainder = buf.len mod WordSize
  if remainder != 0:
    let padding = WordSize - remainder
    for _ in 0 ..< padding:
      buf.add(0)

proc encodeUint32LE*(buf: var seq[byte], val: uint32) {.raises: [].} =
  buf.add(byte(val and 0xFF))
  buf.add(byte((val shr 8) and 0xFF))
  buf.add(byte((val shr 16) and 0xFF))
  buf.add(byte((val shr 24) and 0xFF))

proc encodeUint64LE*(buf: var seq[byte], val: uint64) {.raises: [].} =
  for i in 0 ..< 8:
    buf.add(byte((val shr (i * 8)) and 0xFF))

proc encodeInt64LE*(buf: var seq[byte], val: int64) {.raises: [].} =
  encodeUint64LE(buf, cast[uint64](val))

proc encodeFloat64LE*(buf: var seq[byte], val: float64) {.raises: [].} =
  encodeUint64LE(buf, cast[uint64](val))

proc encodeText*(buf: var seq[byte], s: string) {.raises: [].} =
  ## NUL-terminated string, padded to 8-byte boundary.
  for c in s:
    buf.add(byte(c))
  buf.add(0) # NUL terminator
  padTo8(buf)

# --- Low-level decoding ---

proc decodeUint32LE*(data: openArray[byte], offset: int): uint32 {.raises: [].} =
  if offset + 4 > data.len: return 0
  result = uint32(data[offset]) or
           (uint32(data[offset + 1]) shl 8) or
           (uint32(data[offset + 2]) shl 16) or
           (uint32(data[offset + 3]) shl 24)

proc decodeUint64LE*(data: openArray[byte], offset: int): uint64 {.raises: [].} =
  if offset + 8 > data.len: return 0
  for i in 0 ..< 8:
    result = result or (uint64(data[offset + i]) shl (i * 8))

proc decodeInt64LE*(data: openArray[byte], offset: int): int64 {.raises: [].} =
  cast[int64](decodeUint64LE(data, offset))

proc decodeFloat64LE*(data: openArray[byte], offset: int): float64 {.raises: [].} =
  cast[float64](decodeUint64LE(data, offset))

proc decodeText*(data: openArray[byte], offset: int): (string, int) {.raises: [].} =
  ## Returns (string, total bytes consumed including padding).
  var s = ""
  var i = offset
  while i < data.len and data[i] != 0:
    s.add(char(data[i]))
    inc i
  inc i # skip NUL
  let consumed = i - offset
  let padded = consumed + ((WordSize - (consumed mod WordSize)) mod WordSize)
  result = (s, padded)

# --- Header encoding/decoding ---

proc encodeHeader*(typeCode: uint8, bodyBytes: int,
                   schema: uint8 = 0): seq[byte] {.raises: [].} =
  let bodyWords = uint32((bodyBytes + WordSize - 1) div WordSize)
  result = newSeq[byte](WordSize)
  result[0] = byte(bodyWords and 0xFF)
  result[1] = byte((bodyWords shr 8) and 0xFF)
  result[2] = byte((bodyWords shr 16) and 0xFF)
  result[3] = byte((bodyWords shr 24) and 0xFF)
  result[4] = typeCode
  result[5] = schema
  result[6] = 0
  result[7] = 0

proc decodeHeader*(data: openArray[byte]): MsgHeader {.raises: [DqliteError].} =
  if data.len < WordSize:
    raise newException(DqliteError, "Header too short: " & $data.len & " bytes")
  result.bodyWords = decodeUint32LE(data, 0)
  result.typeCode = data[4]
  result.schemaRevision = data[5]

# --- High-level message builders ---

proc encodeHandshake*(): seq[byte] {.raises: [].} =
  ## Protocol version 1 as uint64 LE (8 bytes).
  result = newSeq[byte]()
  encodeUint64LE(result, DqliteProtocolVersion)

proc encodeRequestLeader*(): seq[byte] {.raises: [].} =
  ## RequestLeader — empty body.
  result = encodeHeader(reqLeader, 0)

proc encodeRequestClient*(clientId: uint64): seq[byte] {.raises: [].} =
  ## RequestClient with client ID.
  var body = newSeq[byte]()
  encodeUint64LE(body, clientId)
  result = encodeHeader(reqClient, body.len)
  result.add(body)

proc encodeRequestOpen*(name: string, flags: uint64 = 0,
                        vfs: string = ""): seq[byte] {.raises: [].} =
  ## RequestOpen with database name, flags, and VFS name.
  var body = newSeq[byte]()
  encodeText(body, name)
  encodeUint64LE(body, flags)
  encodeText(body, vfs)
  result = encodeHeader(reqOpen, body.len)
  result.add(body)

proc encodeParamsTuple(params: seq[DqliteValue]): seq[byte] {.raises: [].} =
  ## Encode parameter count + type header + values.
  # Parameter count as uint8 (packed in a uint64 word)
  var body = newSeq[byte]()
  # First word: param count (just the count as a single byte in a padded word)
  let count = params.len
  # The params tuple starts with a single byte for param count, but
  # in the actual protocol, it's encoded as part of the tuple header.
  # Format: count (uint8) + type_codes (4-bit per param, packed) + padding + values

  if count == 0:
    # No params: just a zero count byte padded to word boundary
    for _ in 0 ..< WordSize:
      body.add(0)
    return body

  # Header: 1 byte count + ceil(count/2) bytes for type nibbles
  var header = newSeq[byte]()
  header.add(byte(count))
  for i in 0 ..< count:
    let typeCode = byte(params[i].kind.ord)
    if i mod 2 == 0:
      header.add(typeCode)
    else:
      header[^1] = header[^1] or (typeCode shl 4)
  padTo8(header)
  body.add(header)

  # Values (each value padded to word boundary or 8 bytes)
  for param in params:
    case param.kind
    of dvkUnknown: discard
    of dvkInt64:
      encodeInt64LE(body, param.intVal)
    of dvkFloat64:
      encodeFloat64LE(body, param.floatVal)
    of dvkText, dvkISO8601:
      encodeText(body, param.textVal)
    of dvkBlob:
      encodeUint32LE(body, uint32(param.blobVal.len))
      for b in param.blobVal:
        body.add(b)
      padTo8(body)
    of dvkNull:
      encodeUint64LE(body, 0)
    of dvkBoolean:
      encodeUint64LE(body, if param.boolVal: 1 else: 0)
  result = body

proc encodeRequestExecSQL*(dbId: uint32, sql: string,
                           params: seq[DqliteValue] = @[]): seq[byte] {.raises: [].} =
  ## RequestExecSQL: db_id + SQL text + params tuple.
  var body = newSeq[byte]()
  encodeUint64LE(body, uint64(dbId))
  encodeText(body, sql)
  body.add(encodeParamsTuple(params))
  result = encodeHeader(reqExecSQL, body.len)
  result.add(body)

proc encodeRequestQuerySQL*(dbId: uint32, sql: string,
                            params: seq[DqliteValue] = @[]): seq[byte] {.raises: [].} =
  ## RequestQuerySQL: db_id + SQL text + params tuple.
  var body = newSeq[byte]()
  encodeUint64LE(body, uint64(dbId))
  encodeText(body, sql)
  body.add(encodeParamsTuple(params))
  result = encodeHeader(reqQuerySQL, body.len)
  result.add(body)

# --- High-level response decoders ---

proc decodeResponseFailure*(data: openArray[byte]): (uint64, string) {.raises: [DqliteError].} =
  ## Decode failure response: (error_code, message).
  if data.len < 16:
    raise newException(DqliteError, "Failure response too short")
  let code = decodeUint64LE(data, 0)
  let (msg, _) = decodeText(data, 8)
  result = (code, msg)

proc decodeResponseWelcome*(data: openArray[byte]): uint64 {.raises: [DqliteError].} =
  ## Decode welcome response: heartbeat timeout.
  if data.len < 8:
    raise newException(DqliteError, "Welcome response too short")
  result = decodeUint64LE(data, 0)

proc decodeResponseDb*(data: openArray[byte]): uint32 {.raises: [DqliteError].} =
  ## Decode Db response: database ID.
  if data.len < 8:
    raise newException(DqliteError, "Db response too short")
  result = decodeUint32LE(data, 0)

proc decodeResponseNode*(data: openArray[byte]): NodeInfo {.raises: [DqliteError].} =
  ## Decode node response: id + address.
  if data.len < 16:
    raise newException(DqliteError, "Node response too short")
  result.id = decodeUint64LE(data, 0)
  let (addr_str, _) = decodeText(data, 8)
  result.address = addr_str

proc decodeResponseResult*(data: openArray[byte]): ExecResult {.raises: [DqliteError].} =
  ## Decode statement result: lastInsertId + rowsAffected.
  if data.len < 16:
    raise newException(DqliteError, "Result response too short")
  result.lastInsertId = decodeUint64LE(data, 0)
  result.rowsAffected = decodeUint64LE(data, 8)

proc decodeRowValues(data: openArray[byte], offset: int,
                     typeHeader: seq[DqliteValueKind]): (seq[DqliteValue], int) {.raises: [DqliteError].} =
  ## Decode a single row's values based on type header. Returns (values, bytesConsumed).
  var values = newSeq[DqliteValue]()
  var pos = offset
  for kind in typeHeader:
    if pos + 8 > data.len:
      raise newException(DqliteError, "Row data truncated at offset " & $pos)
    case kind
    of dvkUnknown:
      raise newException(DqliteError, "Unknown value type in row data")
    of dvkInt64:
      values.add(DqliteValue(kind: dvkInt64, intVal: decodeInt64LE(data, pos)))
      pos += 8
    of dvkFloat64:
      values.add(DqliteValue(kind: dvkFloat64, floatVal: decodeFloat64LE(data, pos)))
      pos += 8
    of dvkText:
      let (s, consumed) = decodeText(data, pos)
      values.add(DqliteValue(kind: dvkText, textVal: s))
      pos += consumed
    of dvkISO8601:
      let (s, consumed) = decodeText(data, pos)
      values.add(DqliteValue(kind: dvkISO8601, textVal: s))
      pos += consumed
    of dvkBlob:
      let blobLen = int(decodeUint32LE(data, pos))
      pos += 4
      if pos + blobLen > data.len:
        raise newException(DqliteError, "Blob data truncated")
      let blobData = data[pos ..< pos + blobLen]
      values.add(DqliteValue(kind: dvkBlob, blobVal: @blobData))
      pos += blobLen
      let remainder = pos mod WordSize
      if remainder != 0:
        pos += WordSize - remainder
    of dvkNull:
      values.add(DqliteValue(kind: dvkNull))
      pos += 8
    of dvkBoolean:
      let v = decodeUint64LE(data, pos)
      values.add(DqliteValue(kind: dvkBoolean, boolVal: v != 0))
      pos += 8
  result = (values, pos - offset)

proc decodeResponseRows*(data: openArray[byte]): QueryResult {.raises: [DqliteError].} =
  ## Decode rows response: column_count + column_names + row_data[].
  if data.len < 8:
    raise newException(DqliteError, "Rows response too short")

  let columnCount = int(decodeUint64LE(data, 0))
  if columnCount <= 0:
    return QueryResult()

  # Decode column names
  var pos = 8
  for i in 0 ..< columnCount:
    if pos >= data.len:
      raise newException(DqliteError, "Column names truncated")
    let (name, consumed) = decodeText(data, pos)
    result.columns.add(name)
    pos += consumed

  # Decode rows
  # Each row: type header (packed 4-bit type codes) + values
  while pos < data.len:
    # Parse type header for this row
    # First byte: header length info, then nibble-packed type codes
    var typeHeader = newSeq[DqliteValueKind]()
    let headerStart = pos

    # Read type nibbles for columnCount columns
    for i in 0 ..< columnCount:
      if pos >= data.len:
        return result  # body fully consumed, no more rows
      let byteIdx = headerStart + (i div 2)
      if byteIdx >= data.len:
        raise newException(DqliteError, "Row type header truncated at column " & $i)
      let nibble = if i mod 2 == 0:
        data[byteIdx] and 0x0F
      else:
        (data[byteIdx] shr 4) and 0x0F
      if nibble == 0:
        return result  # EOF marker (type code 0 = end of rows)
      # Validate type code
      let kind = case nibble
        of 1: dvkInt64
        of 2: dvkFloat64
        of 3: dvkText
        of 4: dvkBlob
        of 5: dvkNull
        of 11: dvkISO8601
        of 12: dvkBoolean
        else:
          raise newException(DqliteError, "Unknown type code: " & $nibble)
      typeHeader.add(kind)

    if typeHeader.len != columnCount:
      return result

    # Skip type header bytes (padded to word boundary)
    let headerBytes = (columnCount + 1) div 2  # ceil(columnCount / 2)
    let paddedHeaderBytes = headerBytes + ((WordSize - (headerBytes mod WordSize)) mod WordSize)
    pos = headerStart + paddedHeaderBytes

    # Decode values
    let (values, consumed) = decodeRowValues(data, pos, typeHeader)
    result.rows.add(values)
    pos += consumed

# --- Value conversion helper ---

proc dqliteValueToString*(val: DqliteValue): string {.raises: [].} =
  ## Convert a DqliteValue to string representation (Row compatibility).
  case val.kind
  of dvkUnknown: result = ""
  of dvkInt64: result = $val.intVal
  of dvkFloat64: result = $val.floatVal
  of dvkText, dvkISO8601: result = val.textVal
  of dvkBlob:
    result = ""
    for b in val.blobVal:
      result.add(char(b))
  of dvkNull: result = ""
  of dvkBoolean: result = if val.boolVal: "1" else: "0"

proc newDqliteError*(code: uint64, msg: string): ref DqliteError {.raises: [].} =
  result = newException(DqliteError, msg)
  result.code = code
