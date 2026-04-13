# Nim の cast は「黙れ、分かってやってる」機能 — ARC 時代の所有権境界を正しく越える

## TL;DR

Nim の `cast` は型システムの脱出ハッチであり、安全ネットを期待する場所ではない。ARC (`--mm:arc` / `--mm:orc`) 環境で `cast[ref T](pointer)` を使うと、結果の変数は ARC に追跡され、スコープ終了時に `=destroy` が呼ばれる。これを理解せずに `GC_ref`/`GC_unref` と組み合わせると、間欠的な use-after-free に陥る。

本記事では nim-lang/Nim#25733 での Araq のフィードバックを起点に、`cast` の設計思想と ARC 所有権モデルの実践的な使い分けを解説する。

## 発端: 500件並行で間欠クラッシュ

[nim-dqlite](https://github.com/GodsGolemInc/nim-dqlite) の非同期ブリッジでは、Channel 経由でワーカースレッドにデータベースハンドルを渡している。Channel は `pointer` しか運べないため、送信側で `cast[pointer](db)` し、受信側で `cast[MasterDb](msg.dbPtr)` に戻す。

```nim
# 送信側: ref を pointer に変換してチャネルに投入
GC_ref(db)   # refcount 1→2（チャネル越え中に GC されないよう保護）
GC_ref(fut)
taskChannel.send(AsyncMsg(
  dbPtr: cast[pointer](db),
  futPtr: cast[pointer](fut), ...))
```

sequential な `waitFor db.execAsync(...)` では問題なく動く。しかし batched な `waitFor all(futs)` で500件並行にすると、間欠的に `AssertionDefect: not db.isNil - Database not connected` が発生した。

## 原因: cast が作る "見えない所有権"

ARC では、`ref T` 型のローカル変数は**所有者**として扱われる。スコープ終了時に `=destroy` が呼ばれ、refcount がデクリメントされる。

問題は `cast` がこのルールの**例外ではない**ことだ:

```nim
proc execWorker(msg: AsyncMsg) =
  let db = cast[MasterDb](msg.dbPtr)  # ARC: "ref T のローカルが生まれた"
  withLock db.lock:
    db.conn.exec(sql(msg.query), msg.args)
  # ← スコープ終了、ARC が =destroy を呼ぶ → refcount -1
```

`cast` は型変換しかしない。`=copy` も `=sink` も呼ばない。しかし結果が `ref T` である以上、ARC はそのローカル変数をライフタイム管理の対象にする。

### 期待した refcount 推移

```
GC_ref(db)           +1  (1→2)
[worker uses db]      0
GC_unref(db)         -1  (2→1)
Net: ±0 ✓
```

### 実際の推移 (修正前)

```
GC_ref(db)                        +1  (1→2)
execWorker scope exit (=destroy)  -1  (2→1)  ← 意図しない
pollResults scope exit (=destroy) -1  (1→0)  ← 意図しない → 解放!
GC_unref(db)                      -1  (0→-1) ← use-after-free
Net: -2 ✗
```

Worker と Poller の**両方**で `cast[MasterDb]` していたため、`=destroy` が2回走り、`GC_unref` と合わせて3回デクリメント。500件並行ではタイミング次第でオブジェクトが早期解放され、別の操作が解放済みメモリにアクセスする。

## Araq のフィードバック: cast の設計思想

この問題を nim-lang/Nim#25733 として提案した。「`cast[ref T](pointer)` で `{.cursor.}` なしのローカルを作ったとき、コンパイラが警告を出すべきでは？」という提案だ。

Araq の回答は明快だった:

> I don't want warnings on `cast`, it's a `cast`, it's a **"shut up I know what I'm doing"** feature.
> Your code should have used `ptr` if it's unowned memory...
> Your workaround with `.cursor` isn't good either.

つまり:

1. **`cast` は安全ネットを期待する場所ではない** — プログラマが全責任を負う
2. **所有しないメモリには `ptr` を使え** — `ref` にキャストした時点で ARC の管理下に入る
3. **`{.cursor.}` は正しいワークアラウンドではない** — 問題を隠すだけ

## ARC における3つのメモリ参照

Araq のフィードバックを消化すると、ARC 時代の Nim でポインタを扱う際の設計原則が見えてくる:

### 1. `ref T` — 所有参照 (ARC 追跡あり)

```nim
let db = newMasterDb()  # ARC が追跡、スコープ終了で =destroy
```

- ARC がライフタイムを管理する
- `=copy` で refcount +1、`=destroy` で refcount -1
- **所有権を持つ**ことを意味する

### 2. `ptr T` — 非所有参照 (ARC 追跡なし)

```nim
let p = cast[ptr MasterDbObj](rawPointer)  # ARC は無関心
# スコープ終了しても =destroy は呼ばれない
```

- ARC が一切関与しない
- `=destroy` は呼ばれない
- **借用**を意味する — 対象の寿命を別の誰かが保証する

### 3. `cast` — 型変換のみ (所有権セマンティクスは結果型に従う)

```nim
cast[MasterDb](ptr)       # → ref T → ARC 追跡あり → =destroy が走る
cast[ptr MasterDbObj](ptr) # → ptr T → ARC 追跡なし → =destroy は走らない
```

`cast` は型を変換するだけで、所有権セマンティクスに干渉しない。結果の型が `ref T` なら ARC が追跡し、`ptr T` なら追跡しない。これが「shut up I know what I'm doing」の意味だ — **型変換の結果がもたらすセマンティクス上の影響はプログラマの責任**。

## 解法: ptr/ref split パターン

この理解に基づいて、非同期ブリッジを再設計した:

```nim
# Worker (別スレッド): ptr — ARC 追跡なし、フィールドアクセスのみ
proc execWorker(msg: AsyncMsg) {.gcsafe.} =
  let db = cast[ptr MasterDbObj](msg.dbPtr)
  withLock db.lock:
    db.conn.exec(sql(msg.query), msg.args)
  # ← =destroy は呼ばれない ✓

# Poller (メインスレッド): ref — ARC の =destroy に任せる
proc pollResults(fd: AsyncFD): bool {.gcsafe.} =
  let db = cast[MasterDb](res.msg.dbPtr)
  let fut = cast[Future[void]](res.msg.futPtr)
  fut.complete()
  # ← =destroy が走る → refcount 2→1 ✓
```

### 修正後の refcount 推移

```
Send:    GC_ref(db)                    +1  (1→2)
Worker:  cast[ptr MasterDbObj] (借用)   0  (2 のまま)
Poll:    cast[MasterDb] (所有)          0  (2 のまま)
         スコープ終了 → =destroy       -1  (2→1)
Net: ±0 ✓
```

`GC_unref` が不要になった。ARC の `=destroy` が `GC_ref` の分を正確に相殺する。手動のライフタイム管理コードがゼロになったのは、この設計の副次的な成果だ。

## まとめ: cast を使うときの設計原則

| 原則 | 説明 |
|------|------|
| **cast の結果型を意識せよ** | `ref T` なら ARC が追跡する。所有権を持たないなら `ptr T` にキャスト |
| **=destroy の発火点を一箇所に限定** | 複数の関数が同じオブジェクトを `cast[ref T]` すると、それぞれで =destroy が走る |
| **`{.cursor.}` でごまかさない** | 問題を隠すだけ。所有権の設計を見直すのが正しい |
| **`GC_ref`/`GC_unref` は最小限に** | ptr/ref split が正しければ手動管理は不要になることが多い |
| **`cast` は安全ネットではない** | 警告もガードレールも出ない。型変換の影響は全てプログラマの責任 |

`cast` は Nim における最も強力で最も危険な機能の一つだ。ARC 時代においては、型変換が暗黙にライフタイム管理を伴うため、旧来の GC 時代よりもさらに慎重な理解が求められる。「shut up I know what I'm doing」と宣言するなら、本当に分かっている必要がある。

---

- 関連 issue: [nim-lang/Nim#25733](https://github.com/nim-lang/Nim/issues/25733)
- 設計記録: [ADR-0003: ARC Memory Safety for cast\[ref\](pointer)](../adr/0003-arc-cursor-for-cast-pointer.md)
- プロジェクト: [nim-dqlite](https://github.com/GodsGolemInc/nim-dqlite) — SQLite/dqlite 医療マスタデータベース抽象
