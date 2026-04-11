## Database abstraction layer.
##
## Provides a unified interface over SQLite/dqlite with a high-performance
## multi-threaded async bridge.

import db_connector/db_sqlite as sqlite
export sqlite.DbError
import std/[strutils, sequtils, asyncdispatch, locks, cpuinfo, nativesockets]

when defined(posix):
  import std/posix

type
  DbBackend* = enum
    dbSqlite = "sqlite"
    dbDqlite = "dqlite"

  MasterDbObj = object
    backend*: DbBackend
    conn: sqlite.DbConn
    path*: string
    lock: Lock

  MasterDb* = ref MasterDbObj

  Row* = seq[string]

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

proc open*(path: string, backend: DbBackend = dbSqlite): MasterDb {.raises: [sqlite.DbError].} =
  ## Open a database connection. Initializes the global async worker pool
  ## on first call. Use `close` to release when done.
  initAsyncInfrastructure()
  let conn = case backend
  of dbSqlite:
    sqlite.open(path, "", "", "")
  of dbDqlite:
    # TODO: dqlite wire protocol support — currently falls back to SQLite
    sqlite.open(path, "", "", "")

  let db = MasterDb(backend: backend, conn: conn, path: path)
  initLock(db.lock)
  db

proc close*(db: MasterDb) {.raises: [].} =
  ## Close the database connection and release the lock.
  ## Sets conn to nil to prevent use-after-close. Safe to call multiple times.
  if not db.conn.isNil:
    try:
      db.conn.close()
    except DbError:
      discard  # best-effort close — connection is being released
    db.conn = nil
  deinitLock(db.lock)

proc exec*(db: MasterDb, sqlStr: string, args: varargs[string, `$`]) {.raises: [sqlite.DbError].} =
  ## Execute a SQL statement with parameterized arguments. Thread-safe.
  withLock db.lock:
    db.conn.exec(sqlite.sql(sqlStr), args)

proc getAllRows*(db: MasterDb, query: string,
                args: varargs[string, `$`]): seq[Row] {.raises: [sqlite.DbError].} =
  ## Execute a query and return all result rows. Thread-safe.
  withLock db.lock:
    result = db.conn.getAllRows(sqlite.sql(query), args)

proc getValue*(db: MasterDb, query: string,
               args: varargs[string, `$`]): string {.raises: [sqlite.DbError].} =
  ## Execute a query and return the first column of the first row. Thread-safe.
  withLock db.lock:
    result = db.conn.getValue(sqlite.sql(query), args)

proc tryExec*(db: MasterDb, sqlStr: string, args: varargs[string, `$`]): bool {.raises: [].} =
  ## Try to execute a SQL statement. Returns false on error without raising.
  try:
    withLock db.lock:
      result = db.conn.tryExec(sqlite.sql(sqlStr), args)
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

proc validateColumnName(name: string) {.raises: [sqlite.DbError].} =
  if name.len == 0:
    raise newException(sqlite.DbError, "Empty column name")
  if name[0] notin {'a'..'z', 'A'..'Z', '_'}:
    raise newException(sqlite.DbError, "Invalid column name: " & name)
  for c in name:
    if c notin {'a'..'z', 'A'..'Z', '0'..'9', '_'}:
      raise newException(sqlite.DbError, "Invalid column name: " & name)

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

proc importCsvRows*(db: MasterDb, table: string, headers: seq[string],
                    rows: seq[seq[string]]) {.raises: [sqlite.DbError].} =
  ## Import rows using INSERT OR REPLACE in a transaction with ROLLBACK on error.
  ## Table and column names are validated against whitelist/format rules.
  validateTable(table)
  for h in headers: validateColumnName(h)
  for i, row in rows:
    if row.len != headers.len:
      raise newException(sqlite.DbError,
        "Row " & $i & " has " & $row.len & " columns, expected " & $headers.len)
  let placeholders = headers.mapIt("?").join(", ")
  let cols = headers.join(", ")
  let insertSql = "INSERT OR REPLACE INTO " & table & " (" & cols & ") VALUES (" & placeholders & ")"
  db.exec("BEGIN TRANSACTION")
  try:
    for row in rows:
      db.exec(insertSql, row)
    db.exec("COMMIT")
  except sqlite.DbError:
    discard db.tryExec("ROLLBACK")
    raise

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
  for h in headers: validateColumnName(h)
  for i, row in rows:
    if row.len != headers.len:
      raise newException(sqlite.DbError,
        "Row " & $i & " has " & $row.len & " columns, expected " & $headers.len)
  var actualHeaders = headers
  var actualRows = rows
  if "revision" notin headers:
    actualHeaders.add("revision")
    for i in 0 ..< actualRows.len:
      actualRows[i].add(revision)
  let placeholders = actualHeaders.mapIt("?").join(", ")
  let cols = actualHeaders.join(", ")
  let insertSql = "INSERT OR REPLACE INTO " & table &
                  " (" & cols & ") VALUES (" & placeholders & ")"
  db.exec("BEGIN TRANSACTION")
  try:
    for row in actualRows:
      db.exec(insertSql, row)
    db.exec("COMMIT")
  except sqlite.DbError:
    discard db.tryExec("ROLLBACK")
    raise
