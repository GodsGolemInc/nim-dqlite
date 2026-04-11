import std/[unittest, os, times]
import dqlite

suite "medical master db":
  let testDir = getTempDir() / ("nim-dqlite-test-" & $epochTime().int)

  setup:
    createDir(testDir)

  teardown:
    removeDir(testDir)

  test "Given new db When openMedicalDb Then schema is created":
    let dbPath = testDir / "test.db"
    var db = openMedicalDb(dbPath)
    defer: db.close()

    check db.recordCount("shinryo_koui") == 0
    check db.recordCount("iyakuhin") == 0
    check db.recordCount("byomei") == 0
    check db.recordCount("master_versions") == 0

  test "Given medical db When insert with revision Then data is stored":
    let dbPath = testDir / "test_rev.db"
    var db = openMedicalDb(dbPath)
    defer: db.close()

    db.exec("""INSERT INTO shinryo_koui (code, revision, name, tensu, category)
               VALUES (?, ?, ?, ?, ?)""",
            "111000110", "2024.06", "初診料", "288", "A")

    check db.recordCount("shinryo_koui") == 1
    check db.recordCountByRevision("shinryo_koui", "2024.06") == 1
    check db.recordCountByRevision("shinryo_koui", "2024.10") == 0

  test "Given same code different revisions When insert Then both stored":
    let dbPath = testDir / "test_multi_rev.db"
    var db = openMedicalDb(dbPath)
    defer: db.close()

    db.exec("""INSERT INTO shinryo_koui (code, revision, name, tensu)
               VALUES (?, ?, ?, ?)""",
            "111000110", "2024.06", "初診料", "288")
    db.exec("""INSERT INTO shinryo_koui (code, revision, name, tensu)
               VALUES (?, ?, ?, ?)""",
            "111000110", "2024.10", "初診料", "291")

    check db.recordCount("shinryo_koui") == 2
    check db.recordCountByRevision("shinryo_koui", "2024.06") == 1
    check db.recordCountByRevision("shinryo_koui", "2024.10") == 1

  test "Given importCsvRowsVersioned When no revision column Then adds it":
    let dbPath = testDir / "test_versioned_import.db"
    var db = openMedicalDb(dbPath)
    defer: db.close()

    let headers = @["code", "name", "tensu", "category"]
    let rows = @[
      @["111000110", "初診料", "288", "A"],
      @["112000110", "再診料", "73", "A"],
    ]

    db.importCsvRowsVersioned("shinryo_koui", "2024.06", headers, rows)
    check db.recordCountByRevision("shinryo_koui", "2024.06") == 2

    let vals = db.getAllRows(
      "SELECT name FROM shinryo_koui WHERE code = ? AND revision = ?",
      "111000110", "2024.06")
    check vals[0][0] == "初診料"

  test "Given version When registerVersion Then recorded":
    let dbPath = testDir / "test_version_reg.db"
    var db = openMedicalDb(dbPath)
    defer: db.close()

    db.registerVersion("2024.06", "shinryo_koui", 20, "shinryo_koui.csv", "abc123")

    let versions = db.listVersions("shinryo_koui")
    check versions.len == 1
    check versions[0].revision == "2024.06"
    check versions[0].rowCount == 20
    check versions[0].status == "active"

  test "Given active revision When new import Then previous is superseded":
    let dbPath = testDir / "test_supersede.db"
    var db = openMedicalDb(dbPath)
    defer: db.close()

    db.registerVersion("2024.06", "shinryo_koui", 20, "file.csv", "aaa")
    db.activateRevision("2024.06", "shinryo_koui")

    db.registerVersion("2024.10", "shinryo_koui", 22, "file2.csv", "bbb")
    db.activateRevision("2024.10", "shinryo_koui")

    let versions = db.listVersions("shinryo_koui")
    check versions.len == 2

    let active = db.activeRevision("shinryo_koui")
    check active == "2024.10"

    # Old version is superseded
    for v in versions:
      if v.revision == "2024.06":
        check v.status == "superseded"

  test "Given superseded revision When activate Then switched back":
    let dbPath = testDir / "test_reactivate.db"
    var db = openMedicalDb(dbPath)
    defer: db.close()

    db.importCsvRowsVersioned("byomei", "2024.06",
      @["code", "name", "icd10"], @[@["8830001", "糖尿病", "E14"]])
    db.registerVersion("2024.06", "byomei", 1, "b.csv", "aaa")

    db.importCsvRowsVersioned("byomei", "2024.10",
      @["code", "name", "icd10"], @[@["8830001", "糖尿病", "E14"]])
    db.registerVersion("2024.10", "byomei", 1, "b.csv", "bbb")
    db.activateRevision("2024.10", "byomei")

    check db.activeRevision("byomei") == "2024.10"

    # Reactivate old revision
    db.activateRevision("2024.06", "byomei")
    check db.activeRevision("byomei") == "2024.06"

  test "Given revision data When purgeRevision Then deleted":
    let dbPath = testDir / "test_purge.db"
    var db = openMedicalDb(dbPath)
    defer: db.close()

    db.importCsvRowsVersioned("iyakuhin", "2024.06",
      @["code", "name", "yakka", "unit"],
      @[@["610001", "テスト薬", "10.0", "錠"]])
    db.registerVersion("2024.06", "iyakuhin", 1, "y.csv", "ccc")

    check db.recordCountByRevision("iyakuhin", "2024.06") == 1

    db.purgeRevision("2024.06", "iyakuhin", "iyakuhin")

    check db.recordCountByRevision("iyakuhin", "2024.06") == 0
    check db.listVersions("iyakuhin").len == 0

  test "Given db When backend Then returns sqlite":
    let dbPath = testDir / "test_backend.db"
    var db = openMedicalDb(dbPath)
    defer: db.close()
    check db.backend == dbSqlite
