# ADR-0002: Channel-based Multi-threaded Async Bridge

## Status
Accepted

## Date
2026-04-11

## Context

SQLite は単一スレッドでのアクセスを前提とするが、非同期アプリケーションからの利用ニーズがある。Nim の `asyncdispatch` はシングルスレッドのイベントループであり、SQLiteのブロッキング I/O をそのまま呼ぶとイベントループが停止する。

要件:
- `asyncdispatch` の `Future[T]` ベースのAPIを提供
- SQLite 操作はブロッキングだが、イベントループをブロックしない
- 複数の非同期操作を並行して発行可能
- スレッド安全性を保証

## Decision

`Channel[AsyncMsg]` + ワーカースレッドプール + `pollResults` タイマーの3層アーキテクチャを採用する。

```
[Async API] --GC_ref+cast[ptr]--> [taskChannel] --> [Worker Threads]
                                                        |
[pollResults timer] <-- [resultChannel] <---------------+
     |
     +--> Future.complete() / Future.fail() + GC_unref
```

1. `execAsync` 等が `Future[T]` を作成し、`GC_ref` で延命させ、`cast[pointer]` でチャネルに送信
2. ワーカースレッドが `taskChannel.recv()` でメッセージを受け取り、`withLock db.lock` で排他実行
3. 結果を `resultChannel` に送信
4. `pollResults` が1msタイマーで結果をポーリングし、`Future` を完了/失敗させて `GC_unref`

ワーカー数は `max(2, countProcessors())` で自動決定。

## Consequences

### Positive
- `asyncdispatch` のイベントループをブロックしない
- 複数DB操作を並行発行可能（ワーカースレッドがキューから取り出して順次実行）
- SQLite のスレッド安全性は `Lock` で保証

### Negative
- `cast[pointer]` + `GC_ref/GC_unref` は低レベルで事故りやすい（→ ADR-0003 で対策）
- 1msポーリングはCPU負荷あり（ただし結果がない場合はすぐ抜ける）
- グローバルなワーカープール（プロセスに1つ）— 複数DB接続でもワーカーは共有

### Risks
- ARC でのメモリ管理との相互作用（軽減策: ADR-0003 の {.cursor.} プラグマ）

## Alternatives Considered

### async/await + プール（malebolgia）
- **却下理由**: malebolgia は計算タスク向け。I/Oブロッキングタスクにはチャネルベースが適切

### 直接スレッド生成（操作ごとに1スレッド）
- **却下理由**: スレッド生成コストが高い。プール方式の方がオーバーヘッドが小さい
