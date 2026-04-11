## Database abstraction layer.
##
## Provides a unified interface over SQLite/dqlite with a high-performance
## multi-threaded async bridge.

import db_connector/db_sqlite as sqlite
from db_connector/sqlite3 import PStmt, reset, clear_bindings, step,
  SQLITE_OK, SQLITE_DONE, SQLITE_ROW
export sqlite.DbError
import std/[strutils, sequtils, asyncdispatch, locks, cpuinfo, nativesockets]
import ./protocol as dqliteProto
import ./client as dqliteClient
export dqliteClient.DqliteConn

when defined(posix):
  import std/posix

type
  DbBackend* = enum
    dbSqlite = "sqlite"
    dbDqlite = "dqlite"

  PragmaConfig* = object
    ## SQLite PRAGMA configuration. Empty string / zero = not set.
    journalMode*: string    ## "WAL", "DELETE", etc.
    synchronous*: string    ## "OFF", "NORMAL", "FULL"
    cacheSize*: int         ## negative = KiB, positive = pages
    mmapSize*: int64        ## bytes
    tempStore*: string      ## "MEMORY", "FILE"
    busyTimeout*: int       ## milliseconds

  MasterDbObj = object
    backend*: DbBackend
    conn: sqlite.DbConn          ## SQLite connection (nil when dbDqlite)
    dqconn: dqliteClient.DqliteConn  ## dqlite connection (nil when dbSqlite)
    path*: string
    lock: Lock

  MasterDb* = ref MasterDbObj

  Row* = seq[string]

  DbValueKind* = enum
    dvText
    dvBlob

  DbValue* = object
    ## A database value that can be either text (bind_text) or binary (bind_blob).
    ## Use `dbText()` and `dbBlob()` constructors.
    case kind*: DbValueKind
    of dvText: textVal*: string
    of dvBlob: blobVal*: seq[byte]

proc dbText*(s: string): DbValue {.raises: [].} =
  ## Create a text DbValue.
  DbValue(kind: dvText, textVal: s)

proc dbBlob*(data: string): DbValue {.raises: [].} =
  ## Create a blob DbValue from a string (e.g. compressed data).
  var bytes = newSeq[byte](data.len)
  for i, c in data:
    bytes[i] = byte(c)
  DbValue(kind: dvBlob, blobVal: bytes)

proc dbBlob*(data: seq[byte]): DbValue {.raises: [].} =
  ## Create a blob DbValue from raw bytes.
  DbValue(kind: dvBlob, blobVal: data)

const
  PragmaDefault* = PragmaConfig()
    ## No PRAGMAs applied. Backwards compatible default.

  PragmaWAL* = PragmaConfig(
    journalMode: "WAL", synchronous: "NORMAL", busyTimeout: 5000)
    ## WAL mode with normal durability. Good for concurrent read/write.

  PragmaPerformance* = PragmaConfig(
    journalMode: "WAL", synchronous: "OFF",
    cacheSize: -64000, mmapSize: 268435456,
    tempStore: "MEMORY", busyTimeout: 5000)
    ## Maximum write throughput. Use for batch imports where crash safety
    ## is handled by rebuild (e.g. --rebuild flag).

# --- Global Async Infrastructure ---

type
  AsyncKind = enum msgExec, msgGetAllRows, msgGetValue, msgQuit
  AsyncMsg = object
    kind: AsyncKind
    dbPtr: pointer
    query: string
    args: seq[string]
    futPtr: pointer

  ResultKind = enum resVoid, resRows, resString, resError
  ResultMsg = object
    kind: ResultKind
    origKind: AsyncKind
    dbPtr: pointer
    futPtr: pointer
    errorMsg: string
    rows: seq[Row]
    val: string

var
  taskChannel: Channel[AsyncMsg]
  resultChannel: Channel[ResultMsg]
  workerThreads: seq[Thread[void]]
  infrastructureInitialized {.global.}: bool = false
  dummyFd: SocketHandle # Persistent handle for macOS Dispatcher

# --- Internal Worker Functions ---

proc execWorker(msg: AsyncMsg) {.gcsafe.} =
  # {.cursor.} prevents ARC from calling =destroy on scope exit for cast-created refs.
  # Without this, each cast[MasterDb](ptr) causes an extra refcount decrement when the
  # local goes out of scope, leading to premature object deallocation under --mm:arc.
  let db {.cursor.} = cast[MasterDb](msg.dbPtr)
  try:
    withLock db.lock:
      db.conn.exec(sqlite.sql(msg.query), msg.args)
    resultChannel.send(ResultMsg(kind: resVoid, origKind: msgExec, dbPtr: msg.dbPtr, futPtr: msg.futPtr))
  except Exception as e:
    resultChannel.send(ResultMsg(kind: resError, origKind: msgExec, dbPtr: msg.dbPtr, futPtr: msg.futPtr, errorMsg: $e.name & ": " & e.msg))

proc getAllRowsWorker(msg: AsyncMsg) {.gcsafe.} =
  let db {.cursor.} = cast[MasterDb](msg.dbPtr)
  try:
    var rows: seq[Row]
    withLock db.lock:
      rows = db.conn.getAllRows(sqlite.sql(msg.query), msg.args)
    resultChannel.send(ResultMsg(kind: resRows, origKind: msgGetAllRows, dbPtr: msg.dbPtr, futPtr: msg.futPtr, rows: rows))
  except Exception as e:
    resultChannel.send(ResultMsg(kind: resError, origKind: msgGetAllRows, dbPtr: msg.dbPtr, futPtr: msg.futPtr, errorMsg: $e.name & ": " & e.msg))

proc getValueWorker(msg: AsyncMsg) {.gcsafe.} =
  let db {.cursor.} = cast[MasterDb](msg.dbPtr)
  try:
    var val: string
    withLock db.lock:
      val = db.conn.getValue(sqlite.sql(msg.query), msg.args)
    resultChannel.send(ResultMsg(kind: resString, origKind: msgGetValue, dbPtr: msg.dbPtr, futPtr: msg.futPtr, val: val))
  except Exception as e:
    resultChannel.send(ResultMsg(kind: resError, origKind: msgGetValue, dbPtr: msg.dbPtr, futPtr: msg.futPtr, errorMsg: $e.name & ": " & e.msg))

proc workerLoop() {.gcsafe.} =
  while true:
    let msg = taskChannel.recv()
    case msg.kind
    of msgExec: execWorker(msg)
    of msgGetAllRows: getAllRowsWorker(msg)
    of msgGetValue: getValueWorker(msg)
    of msgQuit: break

proc pollResults(fd: AsyncFD): bool {.gcsafe.} =
  while true:
    let res = resultChannel.tryRecv()
    if res.dataAvailable:
      let db {.cursor.} = cast[MasterDb](res.msg.dbPtr)
      case res.msg.kind
      of resVoid:
        let fut {.cursor.} = cast[Future[void]](res.msg.futPtr)
        if not fut.finished: fut.complete()
        GC_unref(fut)
      of resRows:
        let fut {.cursor.} = cast[Future[seq[Row]]](res.msg.futPtr)
        if not fut.finished: fut.complete(res.msg.rows)
        GC_unref(fut)
      of resString:
        let fut {.cursor.} = cast[Future[string]](res.msg.futPtr)
        if not fut.finished: fut.complete(res.msg.val)
        GC_unref(fut)
      of resError:
        let err = newException(sqlite.DbError, res.msg.errorMsg)
        case res.msg.origKind
        of msgExec:
          let fut {.cursor.} = cast[Future[void]](res.msg.futPtr)
          if not fut.finished: fut.fail(err)
          GC_unref(fut)
        of msgGetAllRows:
          let fut {.cursor.} = cast[Future[seq[Row]]](res.msg.futPtr)
          if not fut.finished: fut.fail(err)
          GC_unref(fut)
        of msgGetValue:
          let fut {.cursor.} = cast[Future[string]](res.msg.futPtr)
          if not fut.finished: fut.fail(err)
          GC_unref(fut)
        of msgQuit: discard
      GC_unref(db)
    else:
      break
  # Re-schedule timer for polling
  addTimer(1, true, pollResults)
  return false

proc initAsyncInfrastructure() {.raises: [sqlite.DbError].} =
  if infrastructureInitialized: return
  try:
    taskChannel.open()
    resultChannel.open()
    
    # Persistent handle for macOS Dispatcher: Create a non-blocking dummy listener
    dummyFd = createNativeSocket()
    setBlocking(dummyFd, false)
    register(AsyncFD(dummyFd))
    # Keep it registered with a dummy callback that never fires
    addRead(AsyncFD(dummyFd), proc(fd: AsyncFD): bool {.closure, gcsafe.} = true)
    
    # Start polling
    addTimer(1, true, pollResults)

    let numWorkers = max(2, countProcessors())
    workerThreads = newSeq[Thread[void]](numWorkers)
    for i in 0 ..< numWorkers:
      createThread(workerThreads[i], workerLoop)
    
    infrastructureInitialized = true
  except Exception as e:
    raise newException(sqlite.DbError, "Failed to initialize async infrastructure: " & e.msg)

proc shutdownAsyncInfrastructure*() {.raises: [].} =
  ## Gracefully shut down the async worker pool. Sends quit messages to all
  ## workers and joins their threads. Call before process exit for clean shutdown.
  if not infrastructureInitialized: return
  for i in 0 ..< workerThreads.len:
    taskChannel.send(AsyncMsg(kind: msgQuit))
  for i in 0 ..< workerThreads.len:
    joinThread(workerThreads[i])
  taskChannel.close()
  resultChannel.close()
  dummyFd.close()
  infrastructureInitialized = false

# --- Public API ---

proc applyPragmas*(db: MasterDb, config: PragmaConfig) {.raises: [sqlite.DbError].} =
  ## Apply PRAGMA configuration to an open database connection.
  ## Only sets PRAGMAs whose values are non-default (non-empty string / non-zero).
  if config.journalMode.len > 0:
    db.conn.exec(sqlite.sql("PRAGMA journal_mode=" & config.journalMode))
  if config.synchronous.len > 0:
    db.conn.exec(sqlite.sql("PRAGMA synchronous=" & config.synchronous))
  if config.cacheSize != 0:
    db.conn.exec(sqlite.sql("PRAGMA cache_size=" & $config.cacheSize))
  if config.mmapSize != 0:
    db.conn.exec(sqlite.sql("PRAGMA mmap_size=" & $config.mmapSize))
  if config.tempStore.len > 0:
    db.conn.exec(sqlite.sql("PRAGMA temp_store=" & config.tempStore))
  if config.busyTimeout != 0:
    db.conn.exec(sqlite.sql("PRAGMA busy_timeout=" & $config.busyTimeout))

proc open*(path: string, backend: DbBackend = dbSqlite,
           pragmas: PragmaConfig = PragmaDefault): MasterDb {.raises: [sqlite.DbError].} =
  ## Open a database connection with optional PRAGMA configuration.
  ## For dbSqlite: path is a file path.
  ## For dbDqlite: path is comma-separated addresses ("host1:port1,host2:port2").
  ## Initializes the global async worker pool on first call (unless compiled
  ## with -d:noAsyncDb). Use `close` to release.
  case backend
  of dbSqlite:
    when not defined(noAsyncDb):
      initAsyncInfrastructure()
    let conn = sqlite.open(path, "", "", "")
    result = MasterDb(backend: backend, conn: conn, path: path)
    initLock(result.lock)
    if pragmas != PragmaDefault:
      result.applyPragmas(pragmas)
  of dbDqlite:
    try:
      let addresses = path.split(",").mapIt(it.strip())
      let dqconn = dqliteClient.connect(addresses)
      result = MasterDb(backend: backend, dqconn: dqconn, path: path)
      initLock(result.lock)
    except DqliteError as e:
      raise newException(sqlite.DbError, "dqlite: " & e.msg)

proc close*(db: MasterDb) {.raises: [].} =
  ## Close the database connection and release the lock.
  ## Safe to call multiple times.
  case db.backend
  of dbSqlite:
    if not db.conn.isNil:
      try:
        db.conn.close()
      except DbError:
        discard  # best-effort close
      db.conn = nil
  of dbDqlite:
    if not db.dqconn.isNil:
      db.dqconn.close()
      db.dqconn = nil
  deinitLock(db.lock)

proc toDqliteParams(args: openArray[string]): seq[DqliteValue] {.raises: [].} =
  result = newSeq[DqliteValue](args.len)
  for i, a in args:
    result[i] = DqliteValue(kind: dvkText, textVal: a)

proc exec*(db: MasterDb, sqlStr: string, args: varargs[string, `$`]) {.raises: [sqlite.DbError].} =
  ## Execute a SQL statement with parameterized arguments. Thread-safe.
  case db.backend
  of dbSqlite:
    withLock db.lock:
      db.conn.exec(sqlite.sql(sqlStr), args)
  of dbDqlite:
    try:
      withLock db.lock:
        discard db.dqconn.execSQL(sqlStr, toDqliteParams(@args))
    except DqliteError as e:
      raise newException(sqlite.DbError, e.msg)

proc execValues*(db: MasterDb, sqlStr: string,
                 args: openArray[DbValue]) {.raises: [sqlite.DbError].} =
  ## Execute a SQL statement with typed parameters (text or blob).
  ## Uses prepared statements with proper bind_text / bind_blob.
  case db.backend
  of dbSqlite:
    withLock db.lock:
      let stmt = db.conn.prepare(sqlStr)
      try:
        for i, val in args:
          case val.kind
          of dvText:
            sqlite.bindParam(stmt, i + 1, val.textVal)
          of dvBlob:
            sqlite.bindParam(stmt, i + 1, val.blobVal)
        if not sqlite.tryExec(db.conn, stmt):
          sqlite.dbError(db.conn)
      finally:
        sqlite.finalize(stmt)
  of dbDqlite:
    var params = newSeq[DqliteValue](args.len)
    for i, val in args:
      case val.kind
      of dvText:
        params[i] = DqliteValue(kind: dvkText, textVal: val.textVal)
      of dvBlob:
        params[i] = DqliteValue(kind: dvkBlob, blobVal: val.blobVal)
    try:
      withLock db.lock:
        discard db.dqconn.execSQL(sqlStr, params)
    except DqliteError as e:
      raise newException(sqlite.DbError, e.msg)

proc getAllRows*(db: MasterDb, query: string,
                args: varargs[string, `$`]): seq[Row] {.raises: [sqlite.DbError].} =
  ## Execute a query and return all result rows. Thread-safe.
  case db.backend
  of dbSqlite:
    withLock db.lock:
      result = db.conn.getAllRows(sqlite.sql(query), args)
  of dbDqlite:
    try:
      withLock db.lock:
        let qr = db.dqconn.querySQL(query, toDqliteParams(@args))
        for row in qr.rows:
          var r = newSeq[string](row.len)
          for i, val in row:
            r[i] = dqliteValueToString(val)
          result.add(r)
    except DqliteError as e:
      raise newException(sqlite.DbError, e.msg)

proc getValue*(db: MasterDb, query: string,
               args: varargs[string, `$`]): string {.raises: [sqlite.DbError].} =
  ## Execute a query and return the first column of the first row. Thread-safe.
  case db.backend
  of dbSqlite:
    withLock db.lock:
      result = db.conn.getValue(sqlite.sql(query), args)
  of dbDqlite:
    try:
      withLock db.lock:
        let qr = db.dqconn.querySQL(query, toDqliteParams(@args))
        if qr.rows.len > 0 and qr.rows[0].len > 0:
          result = dqliteValueToString(qr.rows[0][0])
        else:
          result = ""
    except DqliteError as e:
      raise newException(sqlite.DbError, e.msg)

proc tryExec*(db: MasterDb, sqlStr: string, args: varargs[string, `$`]): bool {.raises: [].} =
  ## Try to execute a SQL statement. Returns false on error without raising.
  case db.backend
  of dbSqlite:
    try:
      withLock db.lock:
        result = db.conn.tryExec(sqlite.sql(sqlStr), args)
    except CatchableError:
      result = false
  of dbDqlite:
    try:
      withLock db.lock:
        discard db.dqconn.execSQL(sqlStr, toDqliteParams(@args))
      result = true
    except CatchableError:
      result = false

# --- Async API ---

proc execAsync*(db: MasterDb, query: string, args: varargs[string, `$`]): Future[void] {.raises: [].} =
  ## Async version of `exec`. Dispatched to a worker thread.
  let fut = newFuture[void]("execAsync")
  let a = @args
  GC_ref(db)
  GC_ref(fut)
  taskChannel.send(AsyncMsg(kind: msgExec, dbPtr: cast[pointer](db), query: query, args: a, futPtr: cast[pointer](fut)))
  return fut

proc getAllRowsAsync*(db: MasterDb, query: string,
                     args: varargs[string, `$`]): Future[seq[Row]] {.raises: [].} =
  ## Async version of `getAllRows`. Dispatched to a worker thread.
  let fut = newFuture[seq[Row]]("getAllRowsAsync")
  let a = @args
  GC_ref(db)
  GC_ref(fut)
  taskChannel.send(AsyncMsg(kind: msgGetAllRows, dbPtr: cast[pointer](db), query: query, args: a, futPtr: cast[pointer](fut)))
  return fut

proc getValueAsync*(db: MasterDb, query: string,
                   args: varargs[string, `$`]): Future[string] {.raises: [].} =
  ## Async version of `getValue`. Dispatched to a worker thread.
  let fut = newFuture[string]("getValueAsync")
  let a = @args
  GC_ref(db)
  GC_ref(fut)
  taskChannel.send(AsyncMsg(kind: msgGetValue, dbPtr: cast[pointer](db), query: query, args: a, futPtr: cast[pointer](fut)))
  return fut

# --- Medical master schema ---
const VersionsSchema* = """
  CREATE TABLE IF NOT EXISTS master_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    revision TEXT NOT NULL,
    master_type TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    source_file TEXT,
    checksum TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    UNIQUE(revision, master_type)
  );
  CREATE INDEX IF NOT EXISTS idx_versions_status ON master_versions(status);
  CREATE INDEX IF NOT EXISTS idx_versions_revision ON master_versions(revision);
"""

const ShinryoSchema* = """
  CREATE TABLE IF NOT EXISTS shinryo_koui (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    tensu INTEGER,
    category TEXT,
    valid_from TEXT,
    valid_to TEXT,
    PRIMARY KEY (code, revision)
  );
  CREATE TABLE IF NOT EXISTS chouzai (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    tensu INTEGER,
    category TEXT,
    valid_from TEXT,
    valid_to TEXT,
    PRIMARY KEY (code, revision)
  );
  CREATE INDEX IF NOT EXISTS idx_shinryo_category ON shinryo_koui(category);
  CREATE INDEX IF NOT EXISTS idx_shinryo_revision ON shinryo_koui(revision);
  CREATE INDEX IF NOT EXISTS idx_chouzai_revision ON chouzai(revision);
"""

const IyakuhinSchema* = """
  CREATE TABLE IF NOT EXISTS iyakuhin (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    generic_name TEXT,
    yakka REAL,
    unit TEXT,
    category TEXT,
    valid_from TEXT,
    valid_to TEXT,
    PRIMARY KEY (code, revision)
  );
  CREATE TABLE IF NOT EXISTS tokutei_kizai (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    price REAL,
    unit TEXT,
    category TEXT,
    valid_from TEXT,
    valid_to TEXT,
    PRIMARY KEY (code, revision)
  );
  CREATE TABLE IF NOT EXISTS formulary (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    iyakuhin_code TEXT,
    name TEXT NOT NULL,
    adopted_date TEXT,
    status TEXT DEFAULT 'active',
    PRIMARY KEY (code, revision)
  );
  CREATE INDEX IF NOT EXISTS idx_iyakuhin_generic ON iyakuhin(generic_name);
  CREATE INDEX IF NOT EXISTS idx_iyakuhin_revision ON iyakuhin(revision);
  CREATE INDEX IF NOT EXISTS idx_kizai_revision ON tokutei_kizai(revision);
"""

const ByomeiSchema* = """
  CREATE TABLE IF NOT EXISTS byomei (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    icd10 TEXT,
    category TEXT,
    modifier TEXT,
    PRIMARY KEY (code, revision)
  );
  CREATE TABLE IF NOT EXISTS shuushokugo (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    kana TEXT,
    category TEXT,
    PRIMARY KEY (code, revision)
  );
  CREATE INDEX IF NOT EXISTS idx_byomei_icd10 ON byomei(icd10);
  CREATE INDEX IF NOT EXISTS idx_byomei_revision ON byomei(revision);
  CREATE INDEX IF NOT EXISTS idx_shuushokugo_revision ON shuushokugo(revision);
"""

const HoumonKangoSchema* = """
  CREATE TABLE IF NOT EXISTS houmon_kango (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    tensu INTEGER,
    category TEXT,
    valid_from TEXT,
    valid_to TEXT,
    PRIMARY KEY (code, revision)
  );
  CREATE TABLE IF NOT EXISTS houmon_kango_kasan (
    kasan_code TEXT NOT NULL,
    base_code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    valid_from TEXT,
    valid_to TEXT,
    PRIMARY KEY (kasan_code, base_code, revision)
  );
  CREATE TABLE IF NOT EXISTS houmon_kango_kaisu (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    limit_unit TEXT,
    limit_value INTEGER,
    valid_from TEXT,
    valid_to TEXT,
    PRIMARY KEY (code, revision, limit_unit)
  );
  CREATE TABLE IF NOT EXISTS houmon_kango_haihan (
    code1 TEXT NOT NULL,
    code2 TEXT NOT NULL,
    revision TEXT NOT NULL,
    PRIMARY KEY (code1, code2, revision)
  );
  CREATE INDEX IF NOT EXISTS idx_hk_revision ON houmon_kango(revision);
"""

const ShikaSchema* = """
  CREATE TABLE IF NOT EXISTS shika_shinryo (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    tensu INTEGER,
    category TEXT,
    valid_from TEXT,
    valid_to TEXT,
    PRIMARY KEY (code, revision)
  );
  CREATE TABLE IF NOT EXISTS shishiki (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (code, revision)
  );
  CREATE INDEX IF NOT EXISTS idx_shika_revision ON shika_shinryo(revision);
"""

const DpcSchema* = """
  CREATE TABLE IF NOT EXISTS dpc_mdc (
    mdc_code TEXT NOT NULL,
    revision TEXT NOT NULL,
    mdc_name TEXT NOT NULL,
    valid_from TEXT,
    valid_to TEXT,
    PRIMARY KEY (mdc_code, revision)
  );
  CREATE TABLE IF NOT EXISTS dpc_bunrui (
    mdc_code TEXT NOT NULL,
    bunrui_code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    valid_from TEXT,
    valid_to TEXT,
    PRIMARY KEY (mdc_code, bunrui_code, revision)
  );
  CREATE TABLE IF NOT EXISTS dpc_icd (
    mdc_code TEXT NOT NULL,
    bunrui_code TEXT NOT NULL,
    icd_code TEXT NOT NULL,
    revision TEXT NOT NULL,
    icd_name TEXT NOT NULL,
    valid_from TEXT,
    valid_to TEXT,
    PRIMARY KEY (mdc_code, bunrui_code, icd_code, revision)
  );
  CREATE TABLE IF NOT EXISTS dpc_tensu (
    dpc_code TEXT NOT NULL,
    revision TEXT NOT NULL,
    disease_name TEXT,
    operation TEXT,
    period1_days INTEGER,
    period1_points INTEGER,
    period2_days INTEGER,
    period2_points INTEGER,
    valid_from TEXT,
    valid_to TEXT,
    PRIMARY KEY (dpc_code, revision)
  );
  CREATE INDEX IF NOT EXISTS idx_dpc_icd_code ON dpc_icd(icd_code);
"""

const KensaSchema* = """
  CREATE TABLE IF NOT EXISTS kensa (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    jlac10 TEXT,
    category TEXT,
    unit TEXT,
    reference_low REAL,
    reference_high REAL,
    PRIMARY KEY (code, revision)
  );
  CREATE INDEX IF NOT EXISTS idx_kensa_jlac10 ON kensa(jlac10);
"""

const KaigoSchema* = """
  CREATE TABLE IF NOT EXISTS kaigo_service (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    category TEXT,
    tani INTEGER,
    service_type TEXT,
    PRIMARY KEY (code, revision)
  );
"""

const KangoSchema* = """
  CREATE TABLE IF NOT EXISTS kango_jissen (
    code TEXT NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    category TEXT,
    domain TEXT,
    PRIMARY KEY (code, revision)
  );
"""

const MedicalMasterSchema* = VersionsSchema & ShinryoSchema & IyakuhinSchema & ByomeiSchema & HoumonKangoSchema & ShikaSchema & DpcSchema & KensaSchema & KaigoSchema & KangoSchema

const ValidTables* = [
  "master_versions",
  "shinryo_koui", "chouzai",
  "iyakuhin", "tokutei_kizai", "formulary",
  "byomei", "shuushokugo",
  "houmon_kango", "houmon_kango_kasan", "houmon_kango_kaisu", "houmon_kango_haihan",
  "shika_shinryo", "shishiki",
  "dpc_mdc", "dpc_bunrui", "dpc_icd", "dpc_tensu",
  "kensa",
  "kaigo_service",
  "kango_jissen",
]

proc validateTable*(table: string) {.raises: [sqlite.DbError].} =
  ## Validate that a table name is in the known medical schema whitelist.
  ## Raises DbError for unknown table names to prevent SQL injection.
  if table notin ValidTables:
    raise newException(sqlite.DbError, "Invalid table name: " & table)

proc validateIdentifier*(name: string, kind: string) {.raises: [sqlite.DbError].} =
  ## Validate that a SQL identifier (table/column name) contains only safe characters.
  if name.len == 0:
    raise newException(sqlite.DbError, "Empty " & kind & " name")
  if name[0] notin {'a'..'z', 'A'..'Z', '_'}:
    raise newException(sqlite.DbError, "Invalid " & kind & " name: " & name)
  for c in name:
    if c notin {'a'..'z', 'A'..'Z', '0'..'9', '_'}:
      raise newException(sqlite.DbError, "Invalid " & kind & " name: " & name)

proc validateColumnName(name: string) {.raises: [sqlite.DbError].} =
  validateIdentifier(name, "column")

proc initSchema*(db: MasterDb, schema: string) {.raises: [sqlite.DbError].} =
  ## Execute semicolon-delimited schema DDL statements.
  for statement in schema.split(";"):
    let trimmed = statement.strip()
    if trimmed.len > 0:
      db.exec(trimmed)

proc initMedicalSchema*(db: MasterDb) {.raises: [sqlite.DbError].} =
  ## Initialize the full medical master schema (all tables and indexes).
  db.initSchema(MedicalMasterSchema)

proc openMedicalDb*(path: string, backend: DbBackend = dbSqlite): MasterDb {.raises: [sqlite.DbError].} =
  ## Open a database and initialize the full medical master schema.
  let db = open(path, backend)
  initMedicalSchema(db)
  db

proc openDomainDb*(path: string, schema: string, backend: DbBackend = dbSqlite): MasterDb {.raises: [sqlite.DbError].} =
  ## Open a database with a custom schema (VersionsSchema is always included).
  let db = open(path, backend)
  db.initSchema(VersionsSchema & schema)
  db

proc bulkInsert*(db: MasterDb, table: string, columns: seq[string],
                 rows: seq[seq[string]], batchSize: int = 1000,
                 onConflict: string = "OR REPLACE") {.raises: [sqlite.DbError].} =
  ## Generic bulk insert using prepared statements for performance.
  ## - Column names are validated against SQL identifier rules
  ## - Table name validation is the caller's responsibility (use validateTable
  ##   for medical schema, or validateIdentifier for format-only checks)
  ## - Processes `batchSize` rows per transaction
  ## - On error within a batch, rolls back that batch and raises
  for h in columns: validateColumnName(h)
  for i, row in rows:
    if row.len != columns.len:
      raise newException(sqlite.DbError,
        "Row " & $i & " has " & $row.len & " columns, expected " & $columns.len)
  if rows.len == 0: return
  validateIdentifier(table, "table")

  let placeholders = columns.mapIt("?").join(", ")
  let cols = columns.join(", ")
  let insertSql = "INSERT " & onConflict & " INTO " & table &
                  " (" & cols & ") VALUES (" & placeholders & ")"

  case db.backend
  of dbSqlite:
    withLock db.lock:
      let stmt = db.conn.prepare(insertSql)
      try:
        var offset = 0
        while offset < rows.len:
          let batchEnd = min(offset + batchSize, rows.len)
          db.conn.exec(sqlite.sql("BEGIN TRANSACTION"))
          try:
            for i in offset ..< batchEnd:
              if reset(PStmt(stmt)) != SQLITE_OK:
                sqlite.dbError(db.conn)
              if clear_bindings(PStmt(stmt)) != SQLITE_OK:
                sqlite.dbError(db.conn)
              for j, val in rows[i]:
                sqlite.bindParam(stmt, j + 1, val)
              let rc = step(PStmt(stmt))
              if rc notin [SQLITE_DONE, SQLITE_ROW]:
                sqlite.dbError(db.conn)
            db.conn.exec(sqlite.sql("COMMIT"))
          except CatchableError as e:
            discard db.conn.tryExec(sqlite.sql("ROLLBACK"))
            raise newException(sqlite.DbError, e.msg)
          offset = batchEnd
      finally:
        sqlite.finalize(stmt)
  of dbDqlite:
    try:
      withLock db.lock:
        var offset = 0
        while offset < rows.len:
          let batchEnd = min(offset + batchSize, rows.len)
          discard db.dqconn.execSQL("BEGIN TRANSACTION")
          try:
            for i in offset ..< batchEnd:
              discard db.dqconn.execSQL(insertSql, toDqliteParams(rows[i]))
            discard db.dqconn.execSQL("COMMIT")
          except DqliteError as e:
            try: discard db.dqconn.execSQL("ROLLBACK")
            except DqliteError as rollbackErr:
              raise newDqliteError(e.code,
                e.msg & " (ROLLBACK also failed: " & rollbackErr.msg & ")")
            raise
          offset = batchEnd
    except DqliteError as e:
      raise newException(sqlite.DbError, e.msg)

proc bulkInsertValues*(db: MasterDb, table: string, columns: seq[string],
                       rows: seq[seq[DbValue]], batchSize: int = 1000,
                       onConflict: string = "OR REPLACE") {.raises: [sqlite.DbError].} =
  ## Generic bulk insert with typed parameters (text or blob).
  ## Like `bulkInsert` but uses DbValue for proper bind_text / bind_blob.
  for h in columns: validateColumnName(h)
  for i, row in rows:
    if row.len != columns.len:
      raise newException(sqlite.DbError,
        "Row " & $i & " has " & $row.len & " columns, expected " & $columns.len)
  if rows.len == 0: return
  validateIdentifier(table, "table")

  let placeholders = columns.mapIt("?").join(", ")
  let cols = columns.join(", ")
  let insertSql = "INSERT " & onConflict & " INTO " & table &
                  " (" & cols & ") VALUES (" & placeholders & ")"

  case db.backend
  of dbSqlite:
    withLock db.lock:
      let stmt = db.conn.prepare(insertSql)
      try:
        var offset = 0
        while offset < rows.len:
          let batchEnd = min(offset + batchSize, rows.len)
          db.conn.exec(sqlite.sql("BEGIN TRANSACTION"))
          try:
            for i in offset ..< batchEnd:
              if reset(PStmt(stmt)) != SQLITE_OK:
                sqlite.dbError(db.conn)
              if clear_bindings(PStmt(stmt)) != SQLITE_OK:
                sqlite.dbError(db.conn)
              for j, val in rows[i]:
                case val.kind
                of dvText:
                  sqlite.bindParam(stmt, j + 1, val.textVal)
                of dvBlob:
                  sqlite.bindParam(stmt, j + 1, val.blobVal)
              let rc = step(PStmt(stmt))
              if rc notin [SQLITE_DONE, SQLITE_ROW]:
                sqlite.dbError(db.conn)
            db.conn.exec(sqlite.sql("COMMIT"))
          except CatchableError as e:
            discard db.conn.tryExec(sqlite.sql("ROLLBACK"))
            raise newException(sqlite.DbError, e.msg)
          offset = batchEnd
      finally:
        sqlite.finalize(stmt)
  of dbDqlite:
    try:
      withLock db.lock:
        var offset = 0
        while offset < rows.len:
          let batchEnd = min(offset + batchSize, rows.len)
          discard db.dqconn.execSQL("BEGIN TRANSACTION")
          try:
            for i in offset ..< batchEnd:
              var params = newSeq[DqliteValue](rows[i].len)
              for j, val in rows[i]:
                case val.kind
                of dvText:
                  params[j] = DqliteValue(kind: dvkText, textVal: val.textVal)
                of dvBlob:
                  params[j] = DqliteValue(kind: dvkBlob, blobVal: val.blobVal)
              discard db.dqconn.execSQL(insertSql, params)
            discard db.dqconn.execSQL("COMMIT")
          except DqliteError as e:
            try: discard db.dqconn.execSQL("ROLLBACK")
            except DqliteError as rollbackErr:
              raise newDqliteError(e.code,
                e.msg & " (ROLLBACK also failed: " & rollbackErr.msg & ")")
            raise
          offset = batchEnd
    except DqliteError as e:
      raise newException(sqlite.DbError, e.msg)

proc importCsvRows*(db: MasterDb, table: string, headers: seq[string],
                    rows: seq[seq[string]]) {.raises: [sqlite.DbError].} =
  ## Import rows using INSERT OR REPLACE in a single transaction with ROLLBACK
  ## on error. Table name is validated against the medical schema whitelist.
  validateTable(table)
  db.bulkInsert(table, headers, rows, batchSize = rows.len)

proc recordCount*(db: MasterDb, table: string): int {.raises: [sqlite.DbError, ValueError].} =
  ## Return the total number of rows in a table. Table name is validated.
  validateTable(table)
  let val = db.getValue("SELECT COUNT(*) FROM " & table)
  if val.len == 0: 0 else: parseInt(val)

proc recordCountByRevision*(db: MasterDb, table: string,
                            revision: string): int {.raises: [sqlite.DbError, ValueError].} =
  ## Return the number of rows matching a specific revision. Table name is validated.
  validateTable(table)
  let val = db.getValue(
    "SELECT COUNT(*) FROM " & table & " WHERE revision = ?", revision)
  if val.len == 0: 0 else: parseInt(val)

type
  MasterVersion* = object
    id*: int
    revision*: string
    masterType*: string
    importedAt*: string
    rowCount*: int
    sourceFile*: string
    checksum*: string
    status*: string

proc registerVersion*(db: MasterDb, revision, masterType: string,
                      rowCount: int, sourceFile, checksum: string) {.raises: [sqlite.DbError].} =
  ## Register a version in master_versions with status 'active'. Uses INSERT OR REPLACE.
  db.exec("""INSERT OR REPLACE INTO master_versions
             (revision, master_type, imported_at, row_count, source_file, checksum, status)
             VALUES (?, ?, datetime('now'), ?, ?, ?, 'active')""",
          revision, masterType, $rowCount, sourceFile, checksum)

proc listVersions*(db: MasterDb, masterType: string = ""): seq[MasterVersion] {.raises: [sqlite.DbError, ValueError].} =
  ## List versions, optionally filtered by masterType. Ordered by imported_at DESC.
  let query = if masterType.len > 0:
    "SELECT id, revision, master_type, imported_at, row_count, source_file, checksum, status FROM master_versions WHERE master_type = ? ORDER BY imported_at DESC"
  else:
    "SELECT id, revision, master_type, imported_at, row_count, source_file, checksum, status FROM master_versions ORDER BY imported_at DESC"
  let rows = if masterType.len > 0:
    db.getAllRows(query, masterType)
  else:
    db.getAllRows(query)
  for row in rows:
    result.add(MasterVersion(
      id: parseInt(row[0]),
      revision: row[1],
      masterType: row[2],
      importedAt: row[3],
      rowCount: parseInt(row[4]),
      sourceFile: row[5],
      checksum: row[6],
      status: row[7],
    ))

proc activeRevision*(db: MasterDb, masterType: string): string {.raises: [sqlite.DbError].} =
  ## Return the active revision for a masterType, or empty string if none.
  db.getValue(
    "SELECT revision FROM master_versions WHERE master_type = ? AND status = 'active' ORDER BY imported_at DESC LIMIT 1",
    masterType)

proc deactivateRevision*(db: MasterDb, revision, masterType: string) {.raises: [sqlite.DbError].} =
  ## Mark a revision as 'superseded'.
  db.exec(
    "UPDATE master_versions SET status = 'superseded' WHERE revision = ? AND master_type = ?",
    revision, masterType)

proc activateRevision*(db: MasterDb, revision, masterType: string) {.raises: [sqlite.DbError].} =
  ## Atomically activate a revision (deactivates the current active one first).
  db.exec(
    "UPDATE master_versions SET status = 'superseded' WHERE master_type = ? AND status = 'active'",
    masterType)
  db.exec(
    "UPDATE master_versions SET status = 'active' WHERE revision = ? AND master_type = ?",
    revision, masterType)

proc purgeRevision*(db: MasterDb, revision, masterType, table: string) {.raises: [sqlite.DbError].} =
  ## Delete all data for a revision from the specified table and master_versions.
  validateTable(table)
  db.exec("DELETE FROM " & table & " WHERE revision = ?", revision)
  db.exec(
    "DELETE FROM master_versions WHERE revision = ? AND master_type = ?",
    revision, masterType)

proc importCsvRowsVersioned*(db: MasterDb, table: string, revision: string,
                             headers: seq[string],
                             rows: seq[seq[string]]) {.raises: [sqlite.DbError].} =
  ## Like `importCsvRows` but auto-adds a 'revision' column if not in headers.
  validateTable(table)
  var actualHeaders = headers
  var actualRows = rows
  if "revision" notin headers:
    actualHeaders.add("revision")
    for i in 0 ..< actualRows.len:
      actualRows[i].add(revision)
  db.bulkInsert(table, actualHeaders, actualRows, batchSize = actualRows.len)
