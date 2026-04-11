# ADR-0001: SQLite as Default Backend with dqlite for Production HA

## Status
Accepted

## Date
2026-04-11

## Context

医療マスタデータ（診療行為、医薬品、病名等）を管理するデータベースライブラリを設計するにあたり、バックエンドの選択が必要だった。

要件:
- 開発時はゼロ依存でローカル動作すること（外部サーバー不要）
- 本番環境では高可用性（HA）とRaftコンセンサスによるレプリケーション
- 同一APIで開発・本番を切り替えられること

候補:
- SQLite のみ — 単体完結だがHA不可
- dqlite のみ — HA対応だが開発時にサーバー起動が必要
- PostgreSQL — 重量級、医療マスタ参照用途には過剰

## Decision

`DbBackend` enum で SQLite (デフォルト) と dqlite を切り替える二重バックエンド戦略を採用する。
コンパイルフラグ `-d:useDqlite` で dqlite FFI を有効化する。

```nim
type DbBackend* = enum
  dbSqlite = "sqlite"   # 開発・テスト（デフォルト）
  dbDqlite = "dqlite"   # 本番HA環境
```

SQL層は両バックエンドで共通（SQLite互換SQL）。dqlite は SQLite のレプリケーション拡張であり、SQLレベルの互換性がある。

## Consequences

### Positive
- 開発時はゼロセットアップ（`openMedicalDb("file.db")` だけで動作）
- テストは全てSQLiteで実行可能（CI/CDにdqliteサーバー不要）
- 本番移行時にアプリケーションコードの変更が不要

### Negative
- dqlite バックエンドは未実装（TODO: wire protocol 対応）
- dqlite 固有の機能（ノード管理、クラスタ操作）はFFI経由で別途実装が必要

### Risks
- SQLite と dqlite のSQL方言差異（軽減策: CREATE TABLE IF NOT EXISTS 等の標準SQLのみ使用）
