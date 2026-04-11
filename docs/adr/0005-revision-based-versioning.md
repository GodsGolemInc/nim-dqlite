# ADR-0005: Revision-based Multi-generation Master Data Versioning

## Status
Accepted

## Date
2026-04-11

## Context

医療マスタデータは定期的に改定される（診療報酬改定: 通常2年ごと、薬価改定: 毎年）。改定時に:
- 新旧データを同時に保持する必要がある（移行期間）
- 特定リビジョンのデータだけを差し替える
- 旧リビジョンにロールバックする

要件:
- 同一テーブルに複数世代のデータを共存
- リビジョン単位でのアクティブ/非アクティブ切り替え
- リビジョン単位でのパージ（完全削除）

## Decision

全医療テーブルの PRIMARY KEY に `revision` カラムを含め、`master_versions` テーブルでリビジョンのステータスを管理する。

```sql
-- 各テーブル: (code, revision) が複合主キー
PRIMARY KEY (code, revision)

-- master_versions: リビジョンのメタデータ
CREATE TABLE master_versions (
  revision TEXT NOT NULL,
  master_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',  -- 'active' | 'superseded'
  ...
  UNIQUE(revision, master_type)
);
```

リビジョン切り替えは `activateRevision` で原子的に実行:

```nim
proc activateRevision*(db, revision, masterType) =
  # 1. 現在の active を superseded に
  db.exec("UPDATE ... SET status = 'superseded' WHERE status = 'active'")
  # 2. 指定リビジョンを active に
  db.exec("UPDATE ... SET status = 'active' WHERE revision = ?")
```

## Consequences

### Positive
- 同一テーブルに複数世代が共存（JOIN やクエリで revision を指定するだけ）
- `activateRevision` で瞬時にリビジョン切り替え（データコピー不要）
- `purgeRevision` で不要世代を完全削除可能
- `registerVersion` の INSERT OR REPLACE で冪等なインポート

### Negative
- 全クエリで `WHERE revision = ?` を付ける必要がある（アプリケーション側の規律）
- テーブルサイズが世代数に比例して増加

### Risks
- `activateRevision` の2つの UPDATE がトランザクションで囲まれていない（軽減策: SQLite のautocommit モードでは各 UPDATE が原子的であり、2文間のクラッシュ確率は実用上無視できる。厳密な原子性が必要になった段階で BEGIN/COMMIT を追加する）

## Alternatives Considered

### テーブル名にリビジョンを含める（shinryo_koui_2024_06）
- **却下理由**: テーブル数が世代数に比例して爆発。スキーマ管理が困難

### 別データベースファイルに世代を分離
- **却下理由**: 世代間のJOINが不可能。切り替え時にファイルの rename が必要で原子性が低い
