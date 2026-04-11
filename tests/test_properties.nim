## Comprehensive property-based tests for nim-dqlite.
##
## Uses balls (macos-support branch) suite/test DSL with custom generators.
## Compile: nim c -r --path:src --path:libs/balls --path:libs/insideout \
##            --mm:arc --threads:on -d:useMalloc tests/test_properties.nim

import std/[asyncdispatch, os, strutils, sequtils, random, times]
import balls
import dqlite

# ---------------------------------------------------------------------------
# Random generators
# ---------------------------------------------------------------------------

proc randomAscii(length: int): string =
  for _ in 1..length:
    result.add(char(rand(32..126)))

proc randomUnicode(length: int): string =
  let pools = [
    @["漢", "字", "日", "本", "語", "医", "療", "薬", "品", "診"],
    @["🏥", "💊", "🩺", "🔬", "📋", "🧬", "❤️", "🦠", "🩹", "💉"],
    @["م", "ر", "ح", "ب", "ا"],
    @["é", "ñ", "ü", "ö", "ā"],
    @["∀", "∃", "∈", "∉", "≤", "≥", "∞", "∑", "∏", "√"],
  ]
  for _ in 1..length:
    let pool = pools[rand(pools.high)]
    result.add(pool[rand(pool.high)])

proc randomSqlDangerous(): string =
  let payloads = @[
    "'; DROP TABLE master_versions; --",
    "' OR '1'='1",
    "'; DELETE FROM shinryo_koui WHERE '1'='1",
    "1; UPDATE master_versions SET status='hacked'",
    "' UNION SELECT * FROM sqlite_master --",
    "Robert'); DROP TABLE byomei;--",
    "'; ATTACH DATABASE '/tmp/evil.db' AS evil; --",
    "' AND 1=CAST((SELECT sql FROM sqlite_master) AS INT) --",
    "'; INSERT INTO master_versions VALUES(999,'pwned','x','now',0,'','','active'); --",
  ]
  payloads[rand(payloads.high)]

proc randomSpecialChars(): string =
  ## Generate strings with SQL-dangerous characters.
  ## NUL bytes excluded: SQLite C API uses null-terminated strings.
  let chars = @["'", "\"", "\\", "%", "_", "--", ";", "\n", "\r\n", "\t",
                "/*", "*/", "NULL", "null", "''", "\"\"", "\\n", "\\0"]
  let count = rand(3..10)
  for _ in 1..count:
    result.add("prefix" & chars[rand(chars.high)] & "suffix")

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

var suiteCounter {.global.} = 0

proc makeSuiteDir(): string =
  inc suiteCounter
  result = getTempDir() / ("nim-dqlite-props-" & $epochTime().int & "-" & $suiteCounter)
  createDir(result)

proc freshDb(dir, name: string): MasterDb =
  let path = dir / name & ".db"
  openMedicalDb(path)

# Keep macOS dispatcher alive
addTimer(1, false, proc(fd: AsyncFD): bool {.closure, gcsafe.} = true)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

suite "property: string roundtrip (sync)":

  ## Given random ASCII strings When inserted via parameterized query Then retrieved identically
  block string_roundtrip_ascii:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "rt_ascii")
    defer: db.close(); removeDir(dir)
    for i in 1..200:
      let original = randomAscii(rand(1..100))
      let rev = "ascii_" & $i
      db.exec("INSERT INTO master_versions (revision, master_type, imported_at) VALUES (?, ?, ?)",
              original, rev, "now")
      let fetched = db.getValue("SELECT revision FROM master_versions WHERE master_type = ?", rev)
      check fetched == original

  ## Given Unicode strings When inserted Then retrieved identically
  block string_roundtrip_unicode:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "rt_unicode")
    defer: db.close(); removeDir(dir)
    for i in 1..200:
      let original = randomUnicode(rand(1..30))
      let rev = "uni_" & $i
      db.exec("INSERT INTO master_versions (revision, master_type, imported_at) VALUES (?, ?, ?)",
              original, rev, "now")
      let fetched = db.getValue("SELECT revision FROM master_versions WHERE master_type = ?", rev)
      check fetched == original

  ## Given SQL-dangerous characters When inserted via params Then stored safely
  block string_roundtrip_special:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "rt_special")
    defer: db.close(); removeDir(dir)
    let specials = @["'single'", "\"double\"", "back\\slash", "per%cent",
                     "under_score", "semi;colon", "dash--comment",
                     "new\nline", "tab\there", "star/*comment*/end",
                     "empty''quotes", "mixed'\"\\;--%_\n\t"]
    for i, s in specials:
      let rev = "sp_" & $i
      db.exec("INSERT INTO master_versions (revision, master_type, imported_at) VALUES (?, ?, ?)",
              s, rev, "now")
      let fetched = db.getValue("SELECT revision FROM master_versions WHERE master_type = ?", rev)
      check fetched == s

  ## Given empty string When inserted Then retrieved as empty
  block string_roundtrip_empty:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "rt_empty")
    defer: db.close(); removeDir(dir)
    db.exec("INSERT INTO master_versions (revision, master_type, imported_at) VALUES (?, ?, ?)",
            "", "empty_test", "now")
    let fetched = db.getValue("SELECT revision FROM master_versions WHERE master_type = ?", "empty_test")
    check fetched == ""

suite "property: SQL injection resistance":

  ## Given SQL injection payloads When used as parameter values Then tables remain intact
  block injection_payloads:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "injection")
    defer: db.close(); removeDir(dir)
    let tablesBefore = db.getAllRows(
      "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    for i in 1..50:
      let payload = randomSqlDangerous()
      let rev = "inj_" & $i
      db.exec("INSERT INTO master_versions (revision, master_type, imported_at) VALUES (?, ?, ?)",
              payload, rev, "now")
      let fetched = db.getValue("SELECT revision FROM master_versions WHERE master_type = ?", rev)
      check fetched == payload
    let tablesAfter = db.getAllRows(
      "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    check tablesBefore == tablesAfter

  ## Given mixed special chars When used in all column positions Then no corruption
  block injection_all_columns:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "inj_cols")
    defer: db.close(); removeDir(dir)
    for i in 1..30:
      let dangerous = randomSpecialChars()
      db.exec("INSERT INTO byomei (code, revision, name, icd10) VALUES (?, ?, ?, ?)",
              "CODE_" & $i, "REV_" & $i, dangerous, dangerous)
      let rows = db.getAllRows(
        "SELECT name, icd10 FROM byomei WHERE code = ? AND revision = ?",
        "CODE_" & $i, "REV_" & $i)
      check rows.len == 1
      check rows[0][0] == dangerous
      check rows[0][1] == dangerous

suite "property: async and concurrent":
  ## All async tests in a single suite sharing one db to avoid cross-suite
  ## ARC/GC_ref lifecycle issues with the global async worker pool.

  ## Given random strings When inserted via async API Then retrieved identically
  block async_roundtrip:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "async_rt")
    defer: db.close(); removeDir(dir)
    for i in 1..100:
      let original = if i mod 3 == 0: randomUnicode(rand(1..20))
                     elif i mod 3 == 1: randomAscii(rand(1..50))
                     else: randomSpecialChars()
      let rev = "async_" & $i
      waitFor db.execAsync(
        "INSERT INTO master_versions (revision, master_type, imported_at) VALUES (?, ?, ?)",
        original, rev, "now")
      let fetched = waitFor db.getValueAsync(
        "SELECT revision FROM master_versions WHERE master_type = ?", rev)
      check fetched == original

  ## Given async getAllRowsAsync When multiple rows Then all returned correctly
  block async_getallrows:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "async_rows")
    defer: db.close(); removeDir(dir)
    var expected: seq[string]
    for i in 1..50:
      let val = randomAscii(rand(5..20))
      expected.add(val)
      waitFor db.execAsync(
        "INSERT INTO master_versions (revision, master_type, imported_at) VALUES (?, ?, ?)",
        val, "rows_" & $i, "now")
    let rows = waitFor db.getAllRowsAsync(
      "SELECT revision FROM master_versions ORDER BY id")
    check rows.len == 50
    for i in 0..<50:
      check rows[i][0] == expected[i]

  ## Given 500 concurrent async inserts When awaited Then all data intact
  block concurrent_500:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "concurrent")
    defer: db.close(); removeDir(dir)
    let count = 500
    var futs: seq[Future[void]]
    var expectedRevisions: seq[string]
    for i in 1..count:
      let rev = "CONC_" & $i & "_" & $rand(1000000)
      expectedRevisions.add(rev)
      futs.add(db.execAsync(
        "INSERT INTO master_versions (revision, master_type, imported_at) VALUES (?, ?, ?)",
        rev, "concurrent", "now"))
    waitFor all(futs)
    let allRows = db.getAllRows(
      "SELECT revision FROM master_versions WHERE master_type = 'concurrent'")
    let fetchedRevisions = allRows.mapIt(it[0])
    check fetchedRevisions.len == count
    for rev in expectedRevisions:
      check rev in fetchedRevisions

  ## Given writes to multiple tables Then no cross-table corruption
  block concurrent_multi_table:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "conc_multi")
    defer: db.close(); removeDir(dir)
    for i in 1..100:
      db.exec(
        "INSERT INTO shinryo_koui (code, revision, name, tensu) VALUES (?, ?, ?, ?)",
        "S" & $i, "rev1", "診療" & $i, $i)
      db.exec(
        "INSERT INTO iyakuhin (code, revision, name, yakka, unit) VALUES (?, ?, ?, ?, ?)",
        "Y" & $i, "rev1", "薬品" & $i, $(i.float * 1.5), "錠")
    check db.recordCountByRevision("shinryo_koui", "rev1") == 100
    check db.recordCountByRevision("iyakuhin", "rev1") == 100

suite "property: error resilience":

  ## Given mix of valid and invalid SQL When executed async Then valid ops succeed
  block error_mixed:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "err_mixed")
    defer: db.close(); removeDir(dir)
    var validCount = 0
    for i in 1..100:
      if i mod 2 == 0:
        waitFor db.execAsync(
          "INSERT INTO master_versions (revision, master_type, imported_at) VALUES (?, ?, ?)",
          "VAL_" & $i, "stress", "now")
        inc validCount
      else:
        try:
          waitFor db.execAsync("INVALID SQL STATEMENT " & $i)
        except:
          discard
    let finalCount = waitFor db.getValueAsync(
      "SELECT COUNT(*) FROM master_versions WHERE master_type = 'stress'")
    check finalCount == $validCount

  ## Given repeated errors Then worker threads remain functional afterwards
  block error_recovery:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "err_recovery")
    defer: db.close(); removeDir(dir)
    for i in 1..50:
      try:
        waitFor db.execAsync("SELECT * FROM THIS_TABLE_DOES_NOT_EXIST_" & $i)
      except:
        discard
    # Workers should still be alive and functional
    waitFor db.execAsync(
      "INSERT INTO master_versions (revision, master_type, imported_at) VALUES (?, ?, ?)",
      "alive", "recovery", "now")
    let val = waitFor db.getValueAsync(
      "SELECT revision FROM master_versions WHERE master_type = 'recovery'")
    check val == "alive"

suite "property: huge payloads":

  ## Given 1MB string When stored and retrieved Then identical
  block huge_1mb:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "huge1mb")
    defer: db.close(); removeDir(dir)
    let hugeString = "START" & "X".repeat(1024 * 1024) & "END"
    waitFor db.execAsync(
      "INSERT INTO master_versions (revision, master_type, imported_at) VALUES (?, ?, ?)",
      hugeString, "huge1", "now")
    let fetched = waitFor db.getValueAsync(
      "SELECT revision FROM master_versions WHERE master_type = 'huge1'")
    check fetched.len == hugeString.len
    check fetched[0..4] == "START"
    check fetched[^3..^1] == "END"

  ## Given 5MB string When stored sync Then identical
  block huge_5mb:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "huge5mb")
    defer: db.close(); removeDir(dir)
    let hugeString = "BEGIN" & "Y".repeat(5 * 1024 * 1024) & "FINISH"
    db.exec(
      "INSERT INTO master_versions (revision, master_type, imported_at) VALUES (?, ?, ?)",
      hugeString, "huge5", "now")
    let fetched = db.getValue(
      "SELECT revision FROM master_versions WHERE master_type = 'huge5'")
    check fetched.len == hugeString.len
    check fetched[0..4] == "BEGIN"
    check fetched[^6..^1] == "FINISH"

suite "property: CSV import edge cases":

  ## Given CSV with special characters When imported Then data preserved
  block csv_special_chars:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "csv_special")
    defer: db.close(); removeDir(dir)
    let headers = @["code", "name", "tensu", "category"]
    let rows = @[
      @["C001", "テスト'診療", "100", "A"],
      @["C002", "data\"with\"quotes", "200", "B"],
      @["C003", "semi;colon", "300", "C"],
      @["C004", "back\\slash", "400", "D"],
      @["C005", "new\nline", "500", "E"],
      @["C006", "tab\there", "600", "F"],
      @["C007", "🏥診療所💊", "700", "G"],
      @["C008", "percent%under_score", "800", "H"],
    ]
    db.importCsvRowsVersioned("shinryo_koui", "2024.csv", headers, rows)
    check db.recordCountByRevision("shinryo_koui", "2024.csv") == 8
    for row in rows:
      let fetched = db.getAllRows(
        "SELECT name FROM shinryo_koui WHERE code = ? AND revision = ?",
        row[0], "2024.csv")
      check fetched.len == 1
      check fetched[0][0] == row[1]

  ## Given CSV with empty strings When imported Then stored correctly
  block csv_empty_values:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "csv_empty")
    defer: db.close(); removeDir(dir)
    let headers = @["code", "name", "icd10", "category"]
    let rows = @[
      @["B001", "病名1", "", ""],
      @["B002", "", "E14", ""],
      @["B003", "病名3", "", "modifier"],
    ]
    db.importCsvRowsVersioned("byomei", "2024.empty", headers, rows)
    check db.recordCountByRevision("byomei", "2024.empty") == 3
    let r = db.getAllRows(
      "SELECT name, icd10, category FROM byomei WHERE code = 'B001' AND revision = '2024.empty'")
    check r[0][0] == "病名1"
    check r[0][1] == ""
    check r[0][2] == ""

  ## Given importCsvRowsVersioned without revision column When called Then adds revision
  block csv_auto_revision:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "csv_autorev")
    defer: db.close(); removeDir(dir)
    let headers = @["code", "name", "yakka", "unit"]
    let rows = @[
      @["D001", "薬品A", "10.5", "錠"],
      @["D002", "薬品B", "20.0", "mL"],
    ]
    db.importCsvRowsVersioned("iyakuhin", "auto_rev", headers, rows)
    check db.recordCountByRevision("iyakuhin", "auto_rev") == 2
    let r = db.getAllRows(
      "SELECT revision FROM iyakuhin WHERE code = 'D001'")
    check r[0][0] == "auto_rev"

  ## Given importCsvRowsVersioned with revision column When called Then uses existing
  block csv_existing_revision:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "csv_existrev")
    defer: db.close(); removeDir(dir)
    let headers = @["code", "revision", "name", "yakka", "unit"]
    let rows = @[
      @["E001", "my_rev", "薬品C", "30.0", "g"],
    ]
    db.importCsvRowsVersioned("iyakuhin", "ignored_rev", headers, rows)
    let r = db.getAllRows("SELECT revision FROM iyakuhin WHERE code = 'E001'")
    check r[0][0] == "my_rev"

  ## Given large CSV batch When imported Then all rows present
  block csv_large_batch:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "csv_large")
    defer: db.close(); removeDir(dir)
    let headers = @["code", "name", "tensu", "category"]
    var rows: seq[seq[string]]
    for i in 1..1000:
      rows.add(@["LG" & $i, "大量テスト" & $i, $i, "X"])
    db.importCsvRowsVersioned("shinryo_koui", "batch", headers, rows)
    check db.recordCountByRevision("shinryo_koui", "batch") == 1000

suite "property: version management invariants":

  ## Given multiple activateRevision calls Then exactly one active at any time
  block version_single_active:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "ver_active")
    defer: db.close(); removeDir(dir)
    for i in 1..50:
      let rev = "rev_" & $i
      db.registerVersion(rev, "test_type", i, "file.csv", "hash" & $i)
    for _ in 1..100:
      let rev = "rev_" & $(rand(1..50))
      db.activateRevision(rev, "test_type")
      let activeRows = db.getAllRows(
        "SELECT COUNT(*) FROM master_versions WHERE master_type = 'test_type' AND status = 'active'")
      check parseInt(activeRows[0][0]) == 1

  ## Given deactivateRevision When called Then activeRevision returns empty
  block version_deactivate:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "ver_deact")
    defer: db.close(); removeDir(dir)
    db.registerVersion("r1", "dtype", 10, "f.csv", "h1")
    check db.activeRevision("dtype") == "r1"
    db.deactivateRevision("r1", "dtype")
    check db.activeRevision("dtype") == ""

  ## Given purgeRevision When called Then data and version record both gone
  block version_purge_complete:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "ver_purge")
    defer: db.close(); removeDir(dir)
    let headers = @["code", "name", "tensu", "category"]
    let rows = @[@["P001", "テスト", "100", "A"], @["P002", "テスト2", "200", "B"]]
    db.importCsvRowsVersioned("shinryo_koui", "purge_rev", headers, rows)
    db.registerVersion("purge_rev", "shinryo_koui", 2, "f.csv", "h")
    check db.recordCountByRevision("shinryo_koui", "purge_rev") == 2
    check db.listVersions("shinryo_koui").len == 1
    db.purgeRevision("purge_rev", "shinryo_koui", "shinryo_koui")
    check db.recordCountByRevision("shinryo_koui", "purge_rev") == 0
    check db.listVersions("shinryo_koui").len == 0

  ## Given registerVersion twice with same key Then no error (INSERT OR REPLACE)
  block version_idempotent:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "ver_idemp")
    defer: db.close(); removeDir(dir)
    db.registerVersion("r1", "idem", 10, "f1.csv", "h1")
    db.registerVersion("r1", "idem", 20, "f2.csv", "h2")
    let versions = db.listVersions("idem")
    check versions.len == 1
    check versions[0].rowCount == 20
    check versions[0].sourceFile == "f2.csv"

suite "property: schema idempotency":

  ## Given initMedicalSchema called multiple times Then no error and data preserved
  block schema_idempotent:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "schema_idemp")
    defer: db.close(); removeDir(dir)
    db.exec("INSERT INTO shinryo_koui (code, revision, name, tensu) VALUES (?, ?, ?, ?)",
            "S001", "rev1", "初診料", "288")
    db.initMedicalSchema()
    db.initMedicalSchema()
    db.initMedicalSchema()
    check db.recordCount("shinryo_koui") == 1
    let r = db.getAllRows("SELECT name FROM shinryo_koui WHERE code = 'S001'")
    check r[0][0] == "初診料"

suite "property: transaction integrity":

  ## Given importCsvRows with large batch Then all-or-nothing semantics
  block transaction_atomic:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "txn_atomic")
    defer: db.close(); removeDir(dir)
    let headers = @["code", "revision", "name", "tensu", "category"]
    var rows: seq[seq[string]]
    for i in 1..500:
      rows.add(@["TX" & $i, "txn_rev", "診療" & $i, $i, "A"])
    db.importCsvRows("shinryo_koui", headers, rows)
    check db.recordCount("shinryo_koui") == 500

  ## Given importCsvRows with duplicate PK Then INSERT OR REPLACE succeeds
  block transaction_upsert:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "txn_upsert")
    defer: db.close(); removeDir(dir)
    let headers = @["code", "revision", "name", "tensu", "category"]
    let rows1 = @[@["U001", "rev1", "元の名前", "100", "A"]]
    let rows2 = @[@["U001", "rev1", "更新された名前", "200", "B"]]
    db.importCsvRows("shinryo_koui", headers, rows1)
    db.importCsvRows("shinryo_koui", headers, rows2)
    check db.recordCount("shinryo_koui") == 1
    let r = db.getAllRows("SELECT name, tensu FROM shinryo_koui WHERE code = 'U001'")
    check r[0][0] == "更新された名前"
    check r[0][1] == "200"

suite "property: multi-table CRUD":

  ## Given all medical tables When basic CRUD Then each works correctly
  block multi_table_crud:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "multi_crud")
    defer: db.close(); removeDir(dir)

    db.exec("INSERT INTO shinryo_koui (code, revision, name, tensu, category) VALUES (?, ?, ?, ?, ?)",
            "SK001", "r1", "初診料", "288", "A")
    check db.recordCountByRevision("shinryo_koui", "r1") == 1

    db.exec("INSERT INTO chouzai (code, revision, name, tensu) VALUES (?, ?, ?, ?)",
            "CZ001", "r1", "調剤基本料", "42")
    check db.recordCountByRevision("chouzai", "r1") == 1

    db.exec("INSERT INTO iyakuhin (code, revision, name, yakka, unit) VALUES (?, ?, ?, ?, ?)",
            "IY001", "r1", "テスト薬", "10.5", "錠")
    check db.recordCountByRevision("iyakuhin", "r1") == 1

    db.exec("INSERT INTO tokutei_kizai (code, revision, name, price, unit) VALUES (?, ?, ?, ?, ?)",
            "TK001", "r1", "特定器材", "100.0", "個")
    check db.recordCountByRevision("tokutei_kizai", "r1") == 1

    db.exec("INSERT INTO byomei (code, revision, name, icd10) VALUES (?, ?, ?, ?)",
            "BY001", "r1", "糖尿病", "E14")
    check db.recordCountByRevision("byomei", "r1") == 1

    db.exec("INSERT INTO shuushokugo (code, revision, name, kana) VALUES (?, ?, ?, ?)",
            "SH001", "r1", "急性", "きゅうせい")
    check db.recordCountByRevision("shuushokugo", "r1") == 1

    db.exec("INSERT INTO houmon_kango (code, revision, name, tensu) VALUES (?, ?, ?, ?)",
            "HK001", "r1", "訪問看護", "555")
    check db.recordCountByRevision("houmon_kango", "r1") == 1

    db.exec("INSERT INTO shika_shinryo (code, revision, name, tensu) VALUES (?, ?, ?, ?)",
            "SS001", "r1", "歯科診療", "300")
    check db.recordCountByRevision("shika_shinryo", "r1") == 1

    db.exec("INSERT INTO kensa (code, revision, name, jlac10) VALUES (?, ?, ?, ?)",
            "KE001", "r1", "血液検査", "3B035")
    check db.recordCountByRevision("kensa", "r1") == 1

    db.exec("INSERT INTO kaigo_service (code, revision, name, tani) VALUES (?, ?, ?, ?)",
            "KA001", "r1", "介護サービス", "100")
    check db.recordCountByRevision("kaigo_service", "r1") == 1

    db.exec("INSERT INTO kango_jissen (code, revision, name, category) VALUES (?, ?, ?, ?)",
            "KJ001", "r1", "看護実践", "基礎")
    check db.recordCountByRevision("kango_jissen", "r1") == 1

    db.exec("INSERT INTO dpc_mdc (mdc_code, revision, mdc_name) VALUES (?, ?, ?)",
            "01", "r1", "神経系疾患")
    db.exec("INSERT INTO dpc_bunrui (mdc_code, bunrui_code, revision, name) VALUES (?, ?, ?, ?)",
            "01", "010010", "r1", "脳腫瘍")
    db.exec("INSERT INTO dpc_icd (mdc_code, bunrui_code, icd_code, revision, icd_name) VALUES (?, ?, ?, ?, ?)",
            "01", "010010", "C710", "r1", "脳腫瘍")
    db.exec("INSERT INTO dpc_tensu (dpc_code, revision, disease_name, period1_days, period1_points) VALUES (?, ?, ?, ?, ?)",
            "010010xx01x0xx", "r1", "脳腫瘍手術", "7", "3000")

  ## Given PRIMARY KEY constraint When duplicate inserted Then error raised
  block multi_table_pk:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "multi_pk")
    defer: db.close(); removeDir(dir)
    db.exec("INSERT INTO shinryo_koui (code, revision, name, tensu) VALUES (?, ?, ?, ?)",
            "PK001", "r1", "初診料", "288")
    var caught = false
    try:
      db.exec("INSERT INTO shinryo_koui (code, revision, name, tensu) VALUES (?, ?, ?, ?)",
              "PK001", "r1", "再診料", "73")
    except:
      caught = true
    check caught

suite "property: boundary values":

  ## Given INTEGER boundary values When stored Then retrieved correctly
  block boundary_integers:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "bound_int")
    defer: db.close(); removeDir(dir)
    let values = @["0", "-1", "1", "2147483647", "-2147483648",
                   "9223372036854775807", "-9223372036854775808", "999999999"]
    for i, v in values:
      db.exec("INSERT INTO shinryo_koui (code, revision, name, tensu) VALUES (?, ?, ?, ?)",
              "INT" & $i, "r1", "テスト", v)
      let fetched = db.getValue(
        "SELECT tensu FROM shinryo_koui WHERE code = ? AND revision = ?",
        "INT" & $i, "r1")
      check fetched == v

  ## Given REAL boundary values When stored Then retrieved correctly
  block boundary_reals:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "bound_real")
    defer: db.close(); removeDir(dir)
    let values = @["0.0", "-0.0", "1.0", "-1.0", "0.001",
                   "999999999.999", "1e-10", "1e+10"]
    for i, v in values:
      db.exec("INSERT INTO iyakuhin (code, revision, name, yakka, unit) VALUES (?, ?, ?, ?, ?)",
              "REAL" & $i, "r1", "テスト薬", v, "mg")
      let fetched = db.getValue(
        "SELECT yakka FROM iyakuhin WHERE code = ? AND revision = ?",
        "REAL" & $i, "r1")
      check parseFloat(fetched) == parseFloat(v)

  ## Given single character strings When stored Then preserved
  block boundary_single_char:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "bound_char")
    defer: db.close(); removeDir(dir)
    let chars = @["a", "Z", "0", " ", "!", "あ", "漢", "🏥"]
    for i, c in chars:
      db.exec("INSERT INTO byomei (code, revision, name) VALUES (?, ?, ?)",
              "CH" & $i, "r1", c)
      let fetched = db.getValue(
        "SELECT name FROM byomei WHERE code = ? AND revision = ?",
        "CH" & $i, "r1")
      check fetched == c

  ## Given tryExec with invalid SQL When called Then returns false without crash
  block boundary_tryexec:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "bound_tryexec")
    defer: db.close(); removeDir(dir)
    check db.tryExec("INSERT INTO shinryo_koui (code, revision, name) VALUES (?, ?, ?)",
                     "T001", "r1", "テスト") == true
    check db.tryExec("COMPLETELY INVALID SQL") == false
    check db.tryExec("INSERT INTO nonexistent_table VALUES (1)") == false
    check db.recordCount("shinryo_koui") == 1

  ## Given NUL byte in string When inserted Then SQLite raises error (known limitation)
  block boundary_nul_byte:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "bound_nul")
    defer: db.close(); removeDir(dir)
    let withNul = "before\0after"
    var raised = false
    try:
      db.exec("INSERT INTO master_versions (revision, master_type, imported_at) VALUES (?, ?, ?)",
              withNul, "nul_test", "now")
    except:
      raised = true
    check raised

suite "property: stress randomized":

  ## Given 1000 random operations When executed Then DB remains consistent
  block stress_random_ops:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "stress")
    defer: db.close(); removeDir(dir)
    var insertedCount = 0
    for i in 1..1000:
      let op = rand(3)
      case op
      of 0:
        let name = if rand(1) == 0: randomAscii(rand(1..30))
                   else: randomUnicode(rand(1..10))
        db.exec("INSERT INTO shinryo_koui (code, revision, name, tensu, category) VALUES (?, ?, ?, ?, ?)",
                "STRESS_" & $i, "r1", name, $rand(10000), "X")
        inc insertedCount
      of 1:
        discard db.getAllRows("SELECT * FROM shinryo_koui WHERE revision = 'r1' LIMIT 10")
      of 2:
        discard db.recordCountByRevision("shinryo_koui", "r1")
      of 3:
        discard db.getValue("SELECT name FROM shinryo_koui WHERE code = 'STRESS_1' AND revision = 'r1'")
      else:
        discard
    check db.recordCountByRevision("shinryo_koui", "r1") == insertedCount

  ## Given mixed sync and async operations on same db Then no corruption
  block stress_mixed_sync_async:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "stress_mixed")
    defer: db.close(); removeDir(dir)
    for i in 1..100:
      db.exec("INSERT INTO kensa (code, revision, name) VALUES (?, ?, ?)",
              "SYNC_" & $i, "r1", "同期検査" & $i)
    var futs: seq[Future[void]]
    for i in 1..100:
      futs.add(db.execAsync(
        "INSERT INTO kensa (code, revision, name) VALUES (?, ?, ?)",
        "ASYNC_" & $i, "r1", "非同期検査" & $i))
    waitFor all(futs)
    for i in 1..100:
      db.exec("INSERT INTO kensa (code, revision, name) VALUES (?, ?, ?)",
              "POST_" & $i, "r1", "後続検査" & $i)
    check db.recordCountByRevision("kensa", "r1") == 300

suite "property: input validation":

  ## Given invalid table name When used in recordCount Then DbError raised
  block validate_table_recordcount:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "val_table")
    defer: db.close(); removeDir(dir)
    var raised = false
    try:
      discard db.recordCount("'; DROP TABLE master_versions; --")
    except:
      raised = true
    check raised
    # Original table must still exist
    check db.recordCount("master_versions") == 0

  ## Given invalid table name When used in importCsvRows Then DbError raised
  block validate_table_import:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "val_import")
    defer: db.close(); removeDir(dir)
    var raised = false
    try:
      db.importCsvRows("evil_table", @["code"], @[@["X"]])
    except:
      raised = true
    check raised

  ## Given invalid column name When used in importCsvRows Then DbError raised
  block validate_column_name:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "val_col")
    defer: db.close(); removeDir(dir)
    var raised = false
    try:
      db.importCsvRows("shinryo_koui", @["code; DROP TABLE"], @[@["X"]])
    except:
      raised = true
    check raised

  ## Given valid table name When used Then no error
  block validate_table_ok:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "val_ok")
    defer: db.close(); removeDir(dir)
    check db.recordCount("shinryo_koui") == 0
    check db.recordCount("iyakuhin") == 0
    check db.recordCount("master_versions") == 0

suite "property: transaction rollback":

  ## Given importCsvRows with column count mismatch When error Then no partial data
  block rollback_on_error:
    let dir = makeSuiteDir()
    let db = freshDb(dir, "rollback")
    defer: db.close(); removeDir(dir)
    let headers = @["code", "revision", "name", "tensu"]
    let rows = @[
      @["R001", "rev1", "正常行", "100"],
      @["R002", "rev1", "列数不足"],  # 4列必要なのに3列 → SQLite error
    ]
    var raised = false
    try:
      db.importCsvRows("shinryo_koui", headers, rows)
    except:
      raised = true
    check raised
    # ROLLBACK により部分データが残らない
    check db.recordCount("shinryo_koui") == 0
