# ADR-0004: Table Name Whitelist Validation for SQL Injection Prevention

## Status
Accepted

## Date
2026-04-11

## Context

`recordCount`, `purgeRevision`, `importCsvRows` 等で、テーブル名を文字列連結で SQL に埋め込んでいた:

```nim
db.getValue("SELECT COUNT(*) FROM " & table)
db.exec("DELETE FROM " & table & " WHERE revision = ?", revision)
```

SQL ではテーブル名をパラメータ化（`?` プレースホルダ）できない。`table` 引数に `"'; DROP TABLE master_versions; --"` のような攻撃文字列が渡された場合、SQL インジェクションが成立する。

カバレッジ100%のテストでもこの問題は検出されなかった — テストは正当なテーブル名のみを使っていたため、攻撃入力空間がカバーされていなかった。

## Decision

二段階の検証を適用する:

1. **テーブル名**: `ValidTables` ホワイトリスト — 医療スキーマで定義された21テーブルのみ許可
2. **カラム名**: フォーマット検証 — `[a-zA-Z_][a-zA-Z0-9_]*` パターンに合致するもののみ許可

```nim
const ValidTables* = ["master_versions", "shinryo_koui", "chouzai", ...]

proc validateTable*(table: string) {.raises: [DbError].} =
  if table notin ValidTables:
    raise newException(DbError, "Invalid table name: " & table)
```

全ての文字列連結SQL構築箇所（6 proc）の先頭で検証を呼び出す。

## Consequences

### Positive
- SQL インジェクション経路の完全遮断（テーブル名・カラム名）
- 不正入力時に明確なエラーメッセージ

### Negative
- カスタムテーブルを追加する場合、`ValidTables` の更新が必要（拡張性の制約）
- `openDomainDb` でカスタムスキーマを使う場合、`ValidTables` に含まれないテーブルは `importCsvRows` で使えない

### Risks
- `ValidTables` とスキーマ定義の不整合（軽減策: スキーマとホワイトリストが同一ファイルに定義されている）
