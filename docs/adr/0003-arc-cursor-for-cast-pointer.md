# ADR-0003: ARC {.cursor.} Pragma for cast[ref](pointer) Locals

## Status
Accepted

## Date
2026-04-11

## Context

ADR-0002 の非同期ブリッジで `cast[pointer](db)` → チャネル送信 → `cast[MasterDb](ptr)` パターンを使用している。`--mm:arc` 環境でプロパティテスト（500件並行非同期INSERT）を実行したところ、間欠的に `AssertionDefect: not db.isNil - Database not connected` が発生した。

根本原因の分析:

ARC では `cast[MasterDb](ptr)` で作った `let db` がスコープ終了時に `=destroy` を呼び、refcount をデクリメントする。これは `GC_ref`/`GC_unref` とは別の経路であり、各非同期操作で **2回の過剰デクリメント** が発生していた:

```
GC_ref(db)       → +1
execWorker scope exit → -1  (ARC =destroy)
pollResults scope exit → -1  (ARC =destroy)
GC_unref(db)     → -1
合計: +1 -1 -1 -1 = -2 (本来 0 であるべき)
```

sequential `waitFor` では偶然タイミングが合い発現しなかったが、batched `waitFor all(futs)` では急速なrefcount変動でオブジェクトが早期解放された。

## Decision

`cast[ref T](pointer)` で作った全てのローカル変数に `{.cursor.}` プラグマを付与する。

```nim
# ✅ ARC がスコープ終了時に =destroy を呼ばない
let db {.cursor.} = cast[MasterDb](msg.dbPtr)
let fut {.cursor.} = cast[Future[void]](res.msg.futPtr)

# ❌ ARC が =destroy を呼び、余分な refcount デクリメントが発生
let db = cast[MasterDb](msg.dbPtr)
```

`{.cursor.}` は非所有参照を宣言し、ARC の自動 `=destroy` を抑止する。ライフタイム管理は `GC_ref`/`GC_unref` が担う。

## Consequences

### Positive
- batched `waitFor all(futs)` パターンが安全に動作（500件並行テスト通過）
- ARC/ORC の両方で正しいメモリ管理

### Negative
- `{.cursor.}` の付け忘れは静かなメモリ破壊を引き起こす（コンパイラが警告しない）

### Risks
- 将来の非同期操作追加時に `{.cursor.}` を忘れるリスク（軽減策: tdd-cycle 品質ゲートにチェック項目追加済み）

## Alternatives Considered

### ptr T を使い ref T にキャストしない
- **却下理由**: `MasterDb = ref MasterDbObj` のフィールドアクセスに ref 型が必要。ptr での操作は煩雑

### GC_ref を追加で呼んで相殺
- **却下理由**: 呼び出し箇所の数え間違いでリーク or double-free。{.cursor.} の方が宣言的で安全
