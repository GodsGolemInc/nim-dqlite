## nim-dqlite: SQLite/dqlite database abstraction for medical master data.
##
## Development: SQLite backend (default)
## Production: dqlite backend (-d:useDqlite) for HA with Raft consensus
##
## Usage:
##   let db = openMedicalDb("medical_masters.db")
##   defer: db.close()
##   echo db.recordCount("iyakuhin")

import dqlite/db
export db
