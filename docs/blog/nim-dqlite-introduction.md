# nim-dqlite: Nim で医療マスタデータベースを安全に扱う

## なぜ専用ライブラリが必要か

医療マスタデータ（診療報酬点数表、医薬品マスタ、病名マスタ等）には独特の要件がある:

- **多世代共存**: 診療報酬改定（2年ごと）のたびにデータが差し替わるが、移行期間中は新旧データが同時に必要
- **21テーブル・数十万行**: 診療行為、医薬品、病名、DPC、検査、介護サービス等、広範なドメイン
- **ゼロダウンタイム更新**: 本番稼働中にマスタを差し替えられなければならない

nim-dqlite はこれらの要件に特化した SQLite/dqlite 抽象レイヤーで、Nim で書かれている。

## インストール

### nimble から

```bash
nimble install https://github.com/GodsGolemInc/nim-dqlite
```

### ソースから

```bash
git clone https://github.com/GodsGolemInc/nim-dqlite.git
cd nim-dqlite
nimble test  # テスト実行
```

依存:
- Nim >= 2.0.0
- [db_connector](https://github.com/nim-lang/db_connector) >= 0.1.0

テスト実行には追加で [balls](https://github.com/jasagiri/balls/tree/macos-support) と [insideout](https://github.com/jasagiri/insideout/tree/fix-mac-compilation-eintr) が必要（nimble が自動解決）。

## Quick Start

### 同期 API

```nim
import dqlite

# DB を開くとスキーマが自動作成される（21テーブル + インデックス）
let db = openMedicalDb("medical.db")
defer: db.close()

# パラメータ化クエリで安全に INSERT
db.exec("""INSERT INTO shinryo_koui (code, revision, name, tensu, category)
           VALUES (?, ?, ?, ?, ?)""",
        "111000110", "2024.06", "初診料", "288", "A")

echo db.recordCount("shinryo_koui")  # 1
```

全てのクエリがパラメータ化されており、SQL インジェクションの心配がない。テーブル名やカラム名もホワイトリストで検証される（[ADR-0004](../adr/0004-table-name-whitelist-validation.md)）。

### 非同期 API

```nim
import std/asyncdispatch
import dqlite

let db = openMedicalDb("medical.db")
defer: db.close()

# ワーカースレッドプールで非同期実行
waitFor db.execAsync(
  "INSERT INTO iyakuhin (code, revision, name, yakka, unit) VALUES (?, ?, ?, ?, ?)",
  "610001", "2024.06", "ロキソプロフェン", "5.7", "錠")

let rows = waitFor db.getAllRowsAsync(
  "SELECT name, yakka FROM iyakuhin WHERE revision = ?", "2024.06")
```

内部的には `Channel` + ワーカースレッドプールで SQLite のブロッキング I/O を `asyncdispatch` のイベントループから分離している（[ADR-0002](../adr/0002-channel-based-async-bridge.md)）。

## CSV インポートとバージョン管理

医療マスタの典型的なワークフロー: CSV ダウンロード → インポート → バージョン登録 → 活性化。

```nim
# CSV データをインポート（revision カラムは自動付加）
db.importCsvRowsVersioned("shinryo_koui", "2024.06",
  @["code", "name", "tensu", "category"],
  @[
    @["111000110", "初診料", "288", "A"],
    @["112000110", "再診料", "73", "A"],
  ])

# バージョン登録
db.registerVersion("2024.06", "shinryo_koui", 2, "shinryo.csv", "abc123")

# 活性化（前のアクティブリビジョンは自動で superseded に）
db.activateRevision("2024.06", "shinryo_koui")

# 確認
echo db.activeRevision("shinryo_koui")  # "2024.06"
```

リビジョンは `master_versions` テーブルで `active`/`superseded` ステータスとして管理される。同一テーブルに複数世代が共存し、`activateRevision` で瞬時に切り替えられる（[ADR-0005](../adr/0005-revision-based-versioning.md)）。

不要になった旧リビジョンは `purgeRevision` で完全削除:

```nim
db.purgeRevision("2023.04", "shinryo_koui", "shinryo_koui")
```

## スキーマ設計

`openMedicalDb` は21テーブルを自動作成する。ドメインごとに分離したスキーマ定数が提供されているため、必要なドメインだけを使うこともできる:

```nim
# 薬局向け: 医薬品テーブルだけ
let db = openDomainDb("pharmacy.db", IyakuhinSchema)
```

| スキーマ定数 | テーブル |
|---|---|
| `ShinryoSchema` | 診療行為、調剤 |
| `IyakuhinSchema` | 医薬品、特定器材、採用薬 |
| `ByomeiSchema` | 病名、修飾語 |
| `HoumonKangoSchema` | 訪問看護、加算、回数制限、背反 |
| `ShikaSchema` | 歯科診療、歯式 |
| `DpcSchema` | DPC/MDC コード、点数 |
| `KensaSchema` | 検査 |
| `KaigoSchema` | 介護サービス |
| `KangoSchema` | 看護実践 |

## テスト戦略: プロパティベーステストで守る

nim-dqlite は **52件のテスト**（9 unit + 43 property）で品質を担保している。

プロパティベーステストで特に効果的だった検証:

- **SQL インジェクション耐性**: `'; DROP TABLE --` 等の攻撃文字列50件をパラメータ値として投入 → テーブルが無事であることを検証
- **Unicode ラウンドトリップ**: CJK文字、絵文字、アラビア語を200件 INSERT → SELECT で完全一致
- **並行非同期整合性**: 500件の並行 INSERT → 全件存在を検証
- **巨大ペイロード**: 1MB / 5MB 文字列の格納と取得
- **ROLLBACK 検証**: ヘッダー/行数不一致で途中失敗 → 部分データが残らないことを確認

品質ゲート:
- 通過率 100%、SIGSEGV 0、コンパイラ警告 0
- モック/スタブ ゼロ（古典派 TDD — 全テストが実 SQLite DB を使用）
- `discard` / `except` の全箇所を監査し、バグを隠す fallback がないことを確認

## 技術的発見: ARC と cast の罠

開発中に最も興味深かったのは、ARC メモリ管理と `cast[ref T](pointer)` パターンの相互作用で発見したバグだ。

### 症状

500件の並行非同期 INSERT (`waitFor all(futs)`) で間欠的に "Database not connected" がアサーション失敗。sequential な `waitFor` では再現しない。

### 原因

Nim の ARC (`--mm:arc`) では、`cast[MasterDb](ptr)` で作った `let db` がスコープ終了時に `=destroy` を呼ぶ。これは `GC_ref`/`GC_unref` とは別の経路であり、**各非同期操作で2回の余分な refcount デクリメント**が発生していた。

### 修正

`{.cursor.}` プラグマで非所有参照を宣言:

```nim
# ✅ ARC が =destroy を呼ばない
let db {.cursor.} = cast[MasterDb](msg.dbPtr)

# ❌ スコープ終了で =destroy → 二重デクリメント
let db = cast[MasterDb](msg.dbPtr)
```

この発見は [ADR-0003](../adr/0003-arc-cursor-for-cast-pointer.md) に記録し、`/tdd-cycle` と `/security-review` スキルの品質ゲートにも反映した。

**教訓**: `cast[ref T](pointer)` + `GC_ref`/`GC_unref` パターンを ARC 環境で使う場合、`{.cursor.}` は必須。これはカバレッジ100%のテストでは検出できない — sequential 実行では偶然動くが、batched 実行でのみ発現する。

## まとめ

nim-dqlite は:
- **21テーブル**の医療マスタスキーマを自動管理
- **同期/非同期** 両対応のスレッドセーフ API
- **リビジョンベース**の多世代データ管理
- **ホワイトリスト検証** + **ROLLBACK 保護**による安全なデータ操作
- **52件のテスト**（プロパティベーステスト含む）で品質担保

を提供する Nim ライブラリである。

```bash
nimble install https://github.com/GodsGolemInc/nim-dqlite
```

- GitHub: [GodsGolemInc/nim-dqlite](https://github.com/GodsGolemInc/nim-dqlite)
- ADR: [docs/adr/](../adr/)
- License: MIT
