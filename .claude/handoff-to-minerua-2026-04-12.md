# 引き継ぎ: nim-dqlite 機能強化 → Minerua 知識DB 適用

## 作業リポジトリ

- 完了: `foundation/nim-dqlite/` — DB 抽象化レイヤーの機能強化
- 次: `products/minerua/` — 知識 DB ビルドスクリプト・スキーマへの適用

## 今回完了した nim-dqlite の変更

### 1. PRAGMA 自動設定 (PragmaConfig)

手動 PRAGMA 呼び出しが不要になった。

**Before (build_knowledge_db.nim L374-376):**
```nim
gDb.master.exec("PRAGMA journal_mode=WAL")
gDb.master.exec("PRAGMA synchronous=OFF")
gDb.master.exec("PRAGMA cache_size=-64000")
```

**After:**
```nim
let db = open(path, pragmas = PragmaPerformance)
# PragmaPerformance = WAL + synchronous=OFF + cache_size=-64000 + mmap_size=256MB + temp_store=MEMORY
```

**API:**
```nim
# プリセット定数
PragmaDefault       # 何もしない (後方互換)
PragmaWAL           # WAL + synchronous=NORMAL + busy_timeout=5s
PragmaPerformance   # WAL + sync=OFF + cache=64MB + mmap=256MB + temp=MEMORY

# カスタム
let custom = PragmaConfig(journalMode: "WAL", cacheSize: -128000, mmapSize: 30_000_000_000)
let db = open(path, pragmas = custom)

# 既存 DB に後から適用
db.applyPragmas(PragmaPerformance)
```

### 2. BLOB 圧縮ヘルパー (dqlite/compress)

graph_json の圧縮用。別モジュールなので `import dqlite/compress` が必要。

**API:**
```nim
import dqlite/compress

# 明示的な圧縮/展開
let compressed = compressBlob(graphJson)        # deflate 圧縮
let restored = decompressBlob(compressed)       # deflate 展開

# 自動判定 (推奨 — マジックバイト方式)
let stored = compressIfLarger(graphJson, threshold = 1024)
# → 1KB 以下: 0x00 + 元データ
# → 1KB 超:  0x01 + 圧縮データ (圧縮が逆効果なら 0x00 + 元データ)

let restored = decompressAuto(stored)
# → マジックバイトで自動判定。レガシーデータ (プレフィクスなし) もそのまま返す
```

**ig_snapshots への適用例:**
```nim
import dqlite/compress

# 書き込み — execValues + dbBlob で bind_blob を使用 (バイナリ安全)
let graphJson = $ig.toJson()
let compressed = compressIfLarger(graphJson)
gDb.master.execValues(
  "INSERT INTO ig_snapshots (article_id, graph_json, structural_hash) VALUES (?, ?, ?)",
  [dbText($articleId), dbBlob(compressed), dbText(structHash)])

# 読み込み (knowledge_db.nim getArticleGraph)
proc getArticleGraph*(db: KnowledgeDb, articleId: int): string =
  let raw = db.master.getValue("SELECT graph_json FROM ig_snapshots WHERE article_id = ?", $articleId)
  result = decompressAuto(raw)  # 圧縮/非圧縮を自動判定
```

**重要:** 圧縮データにはNULバイトや非UTF-8バイト列が含まれるため:
- `graph_json` カラムの型を TEXT → **BLOB** に変更必須
- 書き込みには `execValues` + `dbBlob()` を使用 (`exec` は `bind_text` を使うため非推奨)
- バルクインサートには `bulkInsertValues` + `dbBlob()` を使用
- `dbText()` はテキストカラム用、`dbBlob()` はバイナリカラム用

### 3. 型付きパラメータ API (DbValue / execValues / bulkInsertValues)

圧縮データ (バイナリ) を安全に格納するための API。`exec` は全パラメータを `bind_text` で送るため、バイナリデータには `execValues` を使用する。

**API:**
```nim
# コンストラクタ
dbText("hello")                    # → bind_text (テキスト用)
dbBlob(compressedData)             # → bind_blob (バイナリ用、string から変換)
dbBlob(byteSeq)                    # → bind_blob (seq[byte] 直接)

# 単一実行
db.execValues("INSERT INTO t (id, data) VALUES (?, ?)",
              [dbText("1"), dbBlob(compressed)])

# バルクインサート (blob 対応版)
var rows: seq[seq[DbValue]]
rows.add(@[dbText($articleId), dbBlob(compressed), dbText(structHash)])
db.bulkInsertValues("ig_snapshots",
                     @["article_id", "graph_json", "structural_hash"],
                     rows, batchSize = 1000)
```

### 4. バルクインサート API (bulkInsert)

prepared statement を内部利用した高速バルク INSERT。医療スキーマに非依存。

**API:**
```nim
proc bulkInsert*(db: MasterDb, table: string, columns: seq[string],
                 rows: seq[seq[string]], batchSize: int = 1000,
                 onConflict: string = "OR REPLACE")
```

**propositions_index への適用例:**
```nim
# Before (build_knowledge_db.nim L270-274): 個別 INSERT × N回
for prop in propositions:
  gDb.master.exec("INSERT OR IGNORE INTO propositions_index ...", prop.id, $articleId, prop.label, $prop.confidence)

# After: バルクインサート (prepared statement 内部利用)
var rows: seq[seq[string]]
for prop in propositions:
  rows.add(@[prop.id, $articleId, prop.label, $prop.confidence])
gDb.master.bulkInsert("propositions_index",
                       @["proposition_id", "article_id", "label", "confidence"],
                       rows, batchSize = 5000, onConflict = "OR IGNORE")
```

**注意:**
- `bulkInsert` は自前でトランザクションを管理する。呼び出し側の BEGIN/COMMIT 内で呼んではならない (SQLite はネストトランザクション非対応)
- テーブル名と列名は SQL identifier バリデーション済み (インジェクション防止)
- テーブル名が医療スキーマに属する場合は `validateTable` を先に呼ぶ (importCsvRows のように)

### 4. 分散バックエンド (dqlite ワイヤプロトコル)

`dbDqlite` バックエンドが実装され、dqlite クラスタに TCP 接続可能になった。

```nim
# ローカル開発 (今まで通り)
let db = open("knowledge.db")

# 分散クラスタ接続
let db = open("node1:9001,node2:9001,node3:9001", dbDqlite)

# API は完全に同一 — exec, getAllRows, getValue, bulkInsert がそのまま動く
db.exec("CREATE TABLE IF NOT EXISTS articles ...")
db.bulkInsert("articles", columns, rows)
```

知識 DB のスケーラビリティが必要になった際に利用可能。

## Minerua 側で必要な変更

### 優先度 1: build_knowledge_db.nim — PRAGMA 置換 + 圧縮適用

**変更箇所:** L374-376

```diff
- gDb.master.exec("PRAGMA journal_mode=WAL")
- gDb.master.exec("PRAGMA synchronous=OFF")
- gDb.master.exec("PRAGMA cache_size=-64000")
+ # PRAGMAs are applied via PragmaPerformance at open time
```

**変更箇所:** openKnowledgeDb 呼び出し (knowledge_db.nim)
```nim
proc openKnowledgeDb*(path: string): KnowledgeDb =
  result.master = open(path, pragmas = PragmaPerformance)  # ← open に pragmas 追加
  ...
```

**変更箇所:** graph_json 書き込み (build_knowledge_db.nim L258-261)
```diff
+ import dqlite/compress
  let graphJson = $ig.toJson()
- gDb.master.exec("INSERT INTO ig_snapshots ...", $articleId, graphJson, structHash)
+ let compressed = compressIfLarger(graphJson)
+ gDb.master.exec("INSERT INTO ig_snapshots ...", $articleId, compressed, structHash)
```

**変更箇所:** graph_json 読み込み (knowledge_db.nim getArticleGraph)
```diff
+ import dqlite/compress
  proc getArticleGraph*(db: KnowledgeDb, articleId: int): string =
    let raw = db.master.getValue("SELECT graph_json FROM ig_snapshots WHERE article_id = ?", $articleId)
-   result = raw
+   result = decompressAuto(raw)
```

### 優先度 2: build_knowledge_db.nim — propositions バルクインサート

**変更箇所:** L220-294 のバッチ処理ロジック

現在の構造:
```
BEGIN TRANSACTION (manual)
  for article in batch:
    INSERT article
    INSERT ig_snapshot
    for prop in propositions:
      INSERT prop  ← 個別 INSERT × N
COMMIT (manual, every 1000 articles)
```

推奨構造:
```
for article in batch:
  INSERT article  (db.exec)
  INSERT ig_snapshot with compressed JSON  (db.exec)
  collect propositions into rows buffer

bulkInsert("propositions_index", columns, rows, batchSize=5000)
# bulkInsert が自前で BEGIN/COMMIT を管理
```

**効果推定:** 50M 件の propositions_index INSERT が prepared statement 化により大幅高速化。

### 優先度 3: build_thesaurus_db.nim — PRAGMA 置換

**変更箇所:** L121-123 を同様に置換。

### 優先度 4: スキーマ最適化 (前回ハンドオフの残項目)

```sql
-- 欠落インデックス追加
CREATE INDEX IF NOT EXISTS idx_ig_article ON ig_snapshots(article_id);
CREATE INDEX IF NOT EXISTS idx_prop_article ON propositions_index(article_id);

-- graph_json を BLOB に変更 (圧縮データ対応)
-- 新規 --rebuild 時にスキーマで BLOB を指定

-- structural_hash 廃止検討 (未使用)

-- propositions_index に PRIMARY KEY 追加検討
-- FTS5 対応
```

### 優先度 5: 全量再ビルド

上記変更を適用後、`--rebuild` で全量再ビルド。

**期待効果:**
- DB サイズ: 25GB → 推定 8-12GB (graph_json 圧縮で 60-70% 削減)
- ビルド速度: propositions の prepared statement 化で改善
- PRAGMA: mmap_size=256MB 追加によるランダムリード改善

## キーファイル対応表

| nim-dqlite (変更済み) | Minerua (要変更) | 適用内容 |
|---|---|---|
| `db.nim:PragmaConfig` | `knowledge_db.nim:openKnowledgeDb` | `open(path, pragmas=PragmaPerformance)` |
| `db.nim:PragmaConfig` | `build_knowledge_db.nim:L374-376` | PRAGMA 手動呼び出し削除 |
| `db.nim:PragmaConfig` | `build_thesaurus_db.nim:L121-123` | 同上 |
| `compress.nim:compressIfLarger` | `build_knowledge_db.nim:L258-261` | graph_json 圧縮書き込み |
| `compress.nim:decompressAuto` | `knowledge_db.nim:getArticleGraph` | graph_json 透過的展開 |
| `db.nim:bulkInsert` | `build_knowledge_db.nim:L270-274` | propositions バルクインサート |
| `db.nim:bulkInsert` | `build_thesaurus_db.nim:L45-54` | synonyms バルクインサート |

## コンパイルコマンド (変更なし)

```bash
cd products/minerua

# ビルドスクリプト
nim c -d:release -d:noAsyncDb --path:src \
  --path:../../foundation/nim-dqlite/src \
  --path:../../foundation/interpretation-graph/src \
  scripts/build_knowledge_db.nim

# テスト
nim c -r -d:noAsyncDb --path:src --path:scripts \
  --path:../../foundation/nim-dqlite/src \
  --path:../../foundation/interpretation-graph/src \
  tests/test_build_knowledge_db.nim
```

## collectPropositions バグについて

前回ハンドオフで報告された collectPropositions バグ (propositions_index に ID 文字列が入る問題) は、現在のコード (build_knowledge_db.nim L176) では `elem.text` を使用しており**修正済み**と確認。再ビルドすれば正しいラベルが格納される。
