# nim-dqlite

SQLite/dqlite database abstraction for medical master data in Nim.

- **Development**: SQLite backend (default)
- **Production**: dqlite backend (`-d:useDqlite`) for HA with Raft consensus

## Install

### From nimble

```bash
nimble install https://github.com/GodsGolemInc/nim-dqlite
```

### From source

```bash
git clone https://github.com/GodsGolemInc/nim-dqlite.git
cd nim-dqlite
nimble test
```

### Dependencies

- Nim >= 2.0.0
- [db_connector](https://github.com/nim-lang/db_connector) >= 0.1.0
- [balls](https://github.com/jasagiri/balls/tree/macos-support) (test only)
- [insideout](https://github.com/jasagiri/insideout/tree/fix-mac-compilation-eintr) (test only, via balls)

### Generate API docs

```bash
nim doc --project --index:on --path:src src/dqlite.nim
# Output: src/htmldocs/
```

## Quick Start

```nim
import dqlite

let db = openMedicalDb("medical.db")
defer: db.close()

# Insert via parameterized query
db.exec("""INSERT INTO shinryo_koui (code, revision, name, tensu, category)
           VALUES (?, ?, ?, ?, ?)""",
        "111000110", "2024.06", "初診料", "288", "A")

# Query
echo db.recordCount("shinryo_koui")  # 1
echo db.activeRevision("shinryo_koui")

# CSV import with automatic revision tracking
db.importCsvRowsVersioned("iyakuhin", "2024.06",
  @["code", "name", "yakka", "unit"],
  @[@["610001", "テスト薬", "10.0", "錠"]])
```

## Async API

Multi-threaded async bridge for non-blocking database operations:

```nim
import std/asyncdispatch
import dqlite

let db = openMedicalDb("medical.db")
defer: db.close()

waitFor db.execAsync("INSERT INTO shinryo_koui (code, revision, name) VALUES (?, ?, ?)",
                     "111000110", "2024.06", "初診料")

let rows = waitFor db.getAllRowsAsync("SELECT * FROM shinryo_koui WHERE revision = ?", "2024.06")
let count = waitFor db.getValueAsync("SELECT COUNT(*) FROM shinryo_koui")
```

## Medical Schema

`openMedicalDb` initializes the full schema automatically:

| Table | Domain |
|---|---|
| `master_versions` | Version tracking |
| `shinryo_koui`, `chouzai` | Medical procedures |
| `iyakuhin`, `tokutei_kizai`, `formulary` | Medications |
| `byomei`, `shuushokugo` | Disease names |
| `houmon_kango`, `houmon_kango_kasan`, `houmon_kango_kaisu`, `houmon_kango_haihan` | Home care nursing |
| `shika_shinryo`, `shishiki` | Dental |
| `dpc_mdc`, `dpc_bunrui`, `dpc_icd`, `dpc_tensu` | DPC/MDC codes |
| `kensa` | Laboratory tests |
| `kaigo_service` | Care services |
| `kango_jissen` | Nursing practices |

For a subset, use `openDomainDb` with a custom schema:

```nim
let db = openDomainDb("pharmacy.db", IyakuhinSchema)
```

## Version Management

```nim
# Register a new version
db.registerVersion("2024.06", "shinryo_koui", 20, "shinryo_koui.csv", "abc123")

# Activate (deactivates previous automatically)
db.activateRevision("2024.06", "shinryo_koui")

# Query active
echo db.activeRevision("shinryo_koui")  # "2024.06"

# Purge old data
db.purgeRevision("2023.04", "shinryo_koui", "shinryo_koui")
```

## Testing

```bash
nimble test
```

Runs 52 tests: 9 unit tests (std/unittest) + 43 property-based tests (balls framework) covering injection resistance, concurrent async, huge payloads, boundary values, transaction rollback, and lifecycle safety.

## Documentation

- [Architecture Decision Records](docs/adr/) (5 ADRs)
- [Introduction blog post](docs/blog/nim-dqlite-introduction.md)

## License

Apache-2.0
