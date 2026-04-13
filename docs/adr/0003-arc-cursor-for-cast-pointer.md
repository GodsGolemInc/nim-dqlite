# ADR-0003: ARC Memory Safety for cast[ref](pointer) in Async Bridge

## Status
Superseded (2026-04-13) — replaces initial {.cursor.} approach based on nim-lang/Nim#25733 feedback

## Date
2026-04-11 (initial), 2026-04-13 (superseded)

## Context

ADR-0002 の非同期ブリッジで `cast[pointer](db)` → チャネル送信 → `cast[MasterDb](ptr)` パターンを使用している。`--mm:arc` 環境でプロパティテスト（500件並行非同期INSERT）を実行したところ、間欠的に `AssertionDefect: not db.isNil - Database not connected` が発生した。

根本原因の分析:

ARC では `cast[MasterDb](ptr)` で作った `let db` がスコープ終了時に `=destroy` を呼び、refcount をデクリメントする。これは `GC_ref`/`GC_unref` とは別の経路であり、worker と poller の**両方**で `=destroy` が走ることで過剰デクリメントが発生していた。

初期対応として `{.cursor.}` プラグマで `=destroy` を抑止し `GC_unref` で手動管理していたが、Nim 作者 Araq から以下のフィードバックを受けた (nim-lang/Nim#25733):

- `cast` は "shut up I know what I'm doing" 機能であり、警告の追加は不適切
- `{.cursor.}` ワークアラウンドも推奨しない
- unowned メモリには `ptr` にキャストすべき

## Decision

**Worker 関数（スレッド内）**: `cast[ptr MasterDbObj]` を使用。フィールドアクセスのみで所有権不要。ARC が追跡しないため `=destroy` は呼ばれない。

**pollResults（メインスレッド）**: `cast[MasterDb]` / `cast[Future[T]]` を使用し、ARC に `=destroy` を任せる。`GC_unref` は不要 — ARC の `=destroy` がスコープ終了時に refcount を自然にデクリメントする。

```nim
# Worker: ptr — ARC 追跡なし
let db = cast[ptr MasterDbObj](msg.dbPtr)

# Poller: ref — ARC の =destroy が GC_ref 分を相殺
let db = cast[MasterDb](res.msg.dbPtr)
let fut = cast[Future[void]](res.msg.futPtr)
```

### Refcount 推移

```
Send:    GC_ref(db)  → refcount 1→2
Worker:  cast[ptr MasterDbObj] → ARC 追跡なし → refcount 2 のまま
Poll:    let db = cast[MasterDb](...) → ARC 追跡あり
         スコープ終了 → =destroy → refcount 2→1 ✓
```

## Consequences

### Positive
- `{.cursor.}` や `GC_unref` の手動管理が不要
- ARC の自然なセマンティクスに沿っている
- batched `waitFor all(futs)` パターンが安全に動作（500件並行テスト通過）

### Negative
- `cast[ref T]` を使う以上、cast の意味を理解する必要がある（Araq: "it's a cast"）

### Risks
- Worker で誤って `ref` にキャストすると二重デクリメントが発生する（軽減策: Worker は `ptr` のみ使用するコメントで明示）
