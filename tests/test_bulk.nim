import std/[unittest, os, times]
import dqlite
import dqlite/compress

suite "bulk insert":
  let testDir = getTempDir() / ("nim-dqlite-bulk-" & $epochTime().int)

  setup:
    createDir(testDir)

  teardown:
    removeDir(testDir)

  test "Given 10 rows batchSize 3 When bulkInsert Then all inserted":
    let dbPath = testDir / "batch.db"
    let db = dqlite.open(dbPath)
    defer: db.close()

    db.exec("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)")

    var rows: seq[seq[string]]
    for i in 0 ..< 10:
      rows.add(@[$i, "item" & $i])

    db.bulkInsert("items", @["id", "name"], rows, batchSize = 3)

    let count = db.getValue("SELECT COUNT(*) FROM items")
    check count == "10"

  test "Given single row When bulkInsert Then inserted":
    let dbPath = testDir / "single.db"
    let db = dqlite.open(dbPath)
    defer: db.close()

    db.exec("CREATE TABLE t (x TEXT)")
    db.bulkInsert("t", @["x"], @[@["hello"]])

    check db.getValue("SELECT x FROM t") == "hello"

  test "Given empty rows When bulkInsert Then no-op":
    let dbPath = testDir / "empty.db"
    let db = dqlite.open(dbPath)
    defer: db.close()

    db.exec("CREATE TABLE t (x TEXT)")
    db.bulkInsert("t", @["x"], @[])

    check db.getValue("SELECT COUNT(*) FROM t") == "0"

  test "Given invalid column name When bulkInsert Then raises DbError":
    let dbPath = testDir / "badcol.db"
    let db = dqlite.open(dbPath)
    defer: db.close()

    db.exec("CREATE TABLE t (x TEXT)")
    expect(DbError):
      db.bulkInsert("t", @["x; DROP TABLE t"], @[@["val"]])

  test "Given mismatched row width When bulkInsert Then raises DbError":
    let dbPath = testDir / "mismatch.db"
    let db = dqlite.open(dbPath)
    defer: db.close()

    db.exec("CREATE TABLE t (a TEXT, b TEXT)")
    expect(DbError):
      db.bulkInsert("t", @["a", "b"], @[@["only_one"]])

  test "Given invalid table name When bulkInsert Then raises DbError":
    let dbPath = testDir / "badtable.db"
    let db = dqlite.open(dbPath)
    defer: db.close()

    expect(DbError):
      db.bulkInsert("'; DROP TABLE t; --", @["x"], @[@["val"]])

  test "Given onConflict IGNORE When duplicate key Then ignored":
    let dbPath = testDir / "ignore.db"
    let db = dqlite.open(dbPath)
    defer: db.close()

    db.exec("CREATE TABLE t (id TEXT PRIMARY KEY, val TEXT)")
    db.bulkInsert("t", @["id", "val"], @[@["1", "first"]])
    db.bulkInsert("t", @["id", "val"], @[@["1", "second"]], onConflict = "OR IGNORE")

    check db.getValue("SELECT val FROM t WHERE id = '1'") == "first"

  test "Given onConflict OR REPLACE When duplicate key Then replaced":
    let dbPath = testDir / "replace.db"
    let db = dqlite.open(dbPath)
    defer: db.close()

    db.exec("CREATE TABLE t (id TEXT PRIMARY KEY, val TEXT)")
    db.bulkInsert("t", @["id", "val"], @[@["1", "first"]])
    db.bulkInsert("t", @["id", "val"], @[@["1", "second"]])

    check db.getValue("SELECT val FROM t WHERE id = '1'") == "second"

  test "Given importCsvRows When called Then still works (backwards compat)":
    let dbPath = testDir / "compat.db"
    let db = openMedicalDb(dbPath)
    defer: db.close()

    let headers = @["code", "revision", "name", "tensu", "category"]
    let rows = @[@["111000110", "2024.06", "初診料", "288", "A"]]
    db.importCsvRows("shinryo_koui", headers, rows)

    check db.getValue("SELECT name FROM shinryo_koui WHERE code = ? AND revision = ?",
                      "111000110", "2024.06") == "初診料"

  test "Given importCsvRowsVersioned When called Then still works (backwards compat)":
    let dbPath = testDir / "compat_ver.db"
    let db = openMedicalDb(dbPath)
    defer: db.close()

    let headers = @["code", "name"]
    let rows = @[@["8830001", "糖尿病"]]
    db.importCsvRowsVersioned("byomei", "2024.06", headers, rows)

    let result = db.getAllRows(
      "SELECT name, revision FROM byomei WHERE code = ?", "8830001")
    check result[0][0] == "糖尿病"
    check result[0][1] == "2024.06"

  test "Given large batch When bulkInsert Then performance with prepared statements":
    let dbPath = testDir / "perf.db"
    let db = dqlite.open(dbPath, pragmas = PragmaPerformance)
    defer: db.close()

    db.exec("CREATE TABLE perf_test (id TEXT, value TEXT)")

    var rows: seq[seq[string]]
    for i in 0 ..< 10000:
      rows.add(@[$i, "value" & $i])

    db.bulkInsert("perf_test", @["id", "value"], rows, batchSize = 2000)

    let count = db.getValue("SELECT COUNT(*) FROM perf_test")
    check count == "10000"

  test "Given blob data When execValues Then stored and retrieved correctly":
    let dbPath = testDir / "blob.db"
    let db = dqlite.open(dbPath)
    defer: db.close()

    db.exec("CREATE TABLE blobs (id TEXT PRIMARY KEY, data BLOB)")

    # Create binary data with NUL bytes and non-UTF8 sequences
    let original = "Hello\x00World\xFF\xFE binary data"
    let compressed = compressIfLarger(original, threshold = 0)  # force compression

    db.execValues("INSERT INTO blobs (id, data) VALUES (?, ?)",
                  [dbText("test1"), dbBlob(compressed)])

    let retrieved = db.getValue("SELECT data FROM blobs WHERE id = ?", "test1")
    let restored = decompressAuto(retrieved)
    check restored == original

  test "Given blob columns When bulkInsertValues Then roundtrip preserved":
    let dbPath = testDir / "bulkblob.db"
    let db = dqlite.open(dbPath)
    defer: db.close()

    db.exec("CREATE TABLE docs (id TEXT, content BLOB)")

    var rows: seq[seq[DbValue]]
    for i in 0 ..< 5:
      let data = "json data " & $i & " with \x00 nul bytes"
      let compressed = compressIfLarger(data, threshold = 0)
      rows.add(@[dbText($i), dbBlob(compressed)])

    db.bulkInsertValues("docs", @["id", "content"], rows)

    let count = db.getValue("SELECT COUNT(*) FROM docs")
    check count == "5"

    let raw = db.getValue("SELECT content FROM docs WHERE id = '2'")
    let restored = decompressAuto(raw)
    check restored == "json data 2 with \x00 nul bytes"
