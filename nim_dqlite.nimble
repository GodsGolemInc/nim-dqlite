# Package
version = "0.1.0"
author = "jasagiri"
description = "dqlite bindings and SQLite-compatible database abstraction for medical master data"
license = "Apache-2.0"

# Dependencies
requires "nim >= 2.0.0"
requires "db_connector >= 0.1.0"
requires "malebolgia >= 1.3.0"
requires "zippy >= 0.10.0"
requires "https://github.com/jasagiri/balls#macos-support"
requires "https://github.com/jasagiri/insideout#fix-mac-compilation-eintr"

srcDir = "src"

task test, "Run tests":
  exec "nim c -r --path:src tests/test_all.nim"
  exec "nim c -r --path:src tests/test_pragma.nim"
  exec "nim c -r --path:src tests/test_compress.nim"
  exec "nim c -r --path:src tests/test_bulk.nim"
  exec "nim c -r --path:src tests/test_protocol.nim"
  exec "nim c -r --path:src tests/test_client.nim"
  exec "nim c -r --path:src --mm:arc --threads:on -d:useMalloc tests/test_properties.nim"
