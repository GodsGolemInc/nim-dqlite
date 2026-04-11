import std/[unittest, os, times]
import dqlite

suite "pragma configuration":
  let testDir = getTempDir() / ("nim-dqlite-pragma-" & $epochTime().int)

  setup:
    createDir(testDir)

  teardown:
    removeDir(testDir)

  test "Given PragmaDefault When open Then no PRAGMAs applied":
    let dbPath = testDir / "default.db"
    let db = dqlite.open(dbPath, pragmas = PragmaDefault)
    defer: db.close()

    # Default journal_mode is "delete" (not WAL)
    let jm = db.getValue("PRAGMA journal_mode")
    check jm == "delete"

  test "Given PragmaWAL When open Then journal_mode is WAL":
    let dbPath = testDir / "wal.db"
    let db = dqlite.open(dbPath, pragmas = PragmaWAL)
    defer: db.close()

    check db.getValue("PRAGMA journal_mode") == "wal"
    check db.getValue("PRAGMA synchronous") == "1"  # NORMAL = 1
    check db.getValue("PRAGMA busy_timeout") == "5000"

  test "Given PragmaPerformance When open Then all performance PRAGMAs set":
    let dbPath = testDir / "perf.db"
    let db = dqlite.open(dbPath, pragmas = PragmaPerformance)
    defer: db.close()

    check db.getValue("PRAGMA journal_mode") == "wal"
    check db.getValue("PRAGMA synchronous") == "0"  # OFF = 0
    check db.getValue("PRAGMA cache_size") == "-64000"
    check db.getValue("PRAGMA mmap_size") == "268435456"
    check db.getValue("PRAGMA busy_timeout") == "5000"

  test "Given custom PragmaConfig When open Then custom values applied":
    let dbPath = testDir / "custom.db"
    let custom = PragmaConfig(
      journalMode: "WAL",
      cacheSize: -32000,
    )
    let db = dqlite.open(dbPath, pragmas = custom)
    defer: db.close()

    check db.getValue("PRAGMA journal_mode") == "wal"
    check db.getValue("PRAGMA cache_size") == "-32000"
    # synchronous not explicitly set — WAL default is NORMAL (1)
    check db.getValue("PRAGMA synchronous") == "1"

  test "Given open db When applyPragmas Then PRAGMAs changed":
    let dbPath = testDir / "apply.db"
    let db = dqlite.open(dbPath)
    defer: db.close()

    check db.getValue("PRAGMA journal_mode") == "delete"

    db.applyPragmas(PragmaWAL)
    check db.getValue("PRAGMA journal_mode") == "wal"

  test "Given open without pragmas param When used Then backwards compatible":
    let dbPath = testDir / "compat.db"
    let db = dqlite.open(dbPath)
    defer: db.close()

    # Should work exactly like before — no PRAGMAs applied
    db.exec("CREATE TABLE t (x TEXT)")
    db.exec("INSERT INTO t VALUES (?)", "hello")
    check db.getValue("SELECT x FROM t") == "hello"
