# Durable Functions ワークフロー解説

## 全体構成

```
HTTP POST /api/orders
        │
        ▼
 StartOrderSaga (HttpTrigger)         ← エントリポイント
        │  scheduleNewOrchestrationInstance
        ▼
 OrderSagaOrchestrator (Orchestrator) ← フロー制御の中枢
        │
        ├── callActivity("CreateOrder")
        ├── callActivity("PublishInventoryReserveCommand")
        │       │ Kafka: inventory.reserve.command
        │       ▼
        │   [InventoryService が処理]
        │       │ Kafka: inventory.reserved / inventory.reservation.failed
        │       ▼
        ├── InventoryReservedConsumer / InventoryReservationFailedConsumer (KafkaTrigger)
        │       │  raiseEvent("InventoryResult")
        │       ▼
        ├── waitForExternalEvent("InventoryResult") ← ここで一時停止
        │
        ├── callActivity("PublishShippingScheduleCommand")
        │       │ Kafka: shipping.schedule.command
        │       ▼
        │   [ShippingService が処理]
        │       │ Kafka: shipping.scheduled
        │       ▼
        ├── ShippingScheduledConsumer (KafkaTrigger)
        │       │  raiseEvent("ShippingResult")
        │       ▼
        └── waitForExternalEvent("ShippingResult") ← ここで一時停止
```

---

## 3 種類のトリガーの役割

### 1. `StartOrderSaga` — HTTP トリガー（エントリポイント）

**ファイル**: [`services/saga-orchestrator/src/main/java/com/example/saga/HttpTriggerFunction.java`](../services/saga-orchestrator/src/main/java/com/example/saga/HttpTriggerFunction.java)

```java
@FunctionName("StartOrderSaga")
public HttpResponseMessage startOrderSaga(
        @HttpTrigger(...)
        HttpRequestMessage<Optional<String>> request,
        @DurableClientInput(name = "durableClient")   // ← DurableTaskClient を注入
        DurableClientContext durableContext, ...) {

    // オーケストレーション インスタンスを起動
    instanceId = client.scheduleNewOrchestrationInstance(
            "OrderSagaOrchestrator",
            new NewOrchestrationInstanceOptions().setInput(input));

    // 202 Accepted + statusQueryGetUri 等を返す
    return durableContext.createCheckStatusResponse(request, instanceId);
}
```

**ポイント**

- `@DurableClientInput` でオーケストレーション管理 API（起動・状態確認・イベント注入）にアクセスする `DurableTaskClient` を取得する。
- `createCheckStatusResponse` が返す 202 レスポンスには `statusQueryGetUri`・`sendEventPostUri` などの管理 URL が含まれる。e2e-test.ps1 の `Wait-Orchestration` はこの URL を使ってポーリングしている。

---

### 2. `OrderSagaOrchestrator` — オーケストレーター（フロー定義）

**ファイル**: [`services/saga-orchestrator/src/main/java/com/example/saga/SagaOrchestratorFunction.java`](../services/saga-orchestrator/src/main/java/com/example/saga/SagaOrchestratorFunction.java)

```java
@FunctionName("OrderSagaOrchestrator")
public void orderSagaOrchestrator(
        @DurableOrchestrationTrigger(name = "rpcResult")   // ← Durable 専用トリガー
        TaskOrchestrationContext ctx) {
```

#### Replay（再実行）の仕組み

Durable Functions のオーケストレーターは **Event Sourcing** で状態を管理する。`await()` が呼ばれるたびにメモリから解放され、次のイベント（Activity 完了・外部イベント到着）が来ると**最初から再実行（Replay）** される。

```
① ctx.callActivity("CreateOrder").await()
   → Activity 実行 → 完了イベントを DTS に保存 → Replay
② ctx.callActivity("PublishInventoryReserveCommand").await()
   → Activity 実行 → 完了イベントを DTS に保存 → Replay
③ ctx.waitForExternalEvent("InventoryResult").await()
   → 外部イベント待機（一時停止）
   → raiseEvent 到着 → Replay し ③ 以降を続行
```

Replay 時は DTS に保存されたイベント履歴を再生するため、Activity が再度実行されることはない。

#### Replay 安全性の制約

| 制約 | 理由 |
|---|---|
| I/O の直接呼び出し禁止 | Replay のたびに実行され副作用が重複する |
| `new Random()` / `UUID.randomUUID()` 禁止 | Replay ごとに異なる値になり分岐が変わる |
| `Instant.now()` は `ctx.getCurrentInstant()` に置き換えるべき | Replay 時刻と初回実行時刻が異なるため |

本コードの `Instant.now()` はコマンドオブジェクトのタイムスタンプ生成に使っているが、この値はオーケストレーターの分岐判定に使われないため Replay の整合性への実害はない。厳密には `ctx.getCurrentInstant()` を使う方が正しい。

#### タイムアウト付き外部イベント待機

```java
// イベント待機タスク
Task<InventoryResultEvent> inventoryEventTask =
        ctx.waitForExternalEvent("InventoryResult", InventoryResultEvent.class);
// タイムアウトタイマータスク（環境変数 SAGA_TIMEOUT_MINUTES、デフォルト 30 分）
Task<Void> inventoryTimerTask =
        ctx.createTimer(ctx.getCurrentInstant().atZone(ZoneOffset.UTC)
                .plus(Duration.ofMinutes(TIMEOUT_MINUTES)));

// どちらか先に完了したほうが winner
Task<?> inventoryWinner = ctx.anyOf(inventoryEventTask, inventoryTimerTask).await();

if (inventoryWinner == inventoryTimerTask) {
    // タイムアウト → 補償して終了
    ctx.callActivity("PublishOrderCancelled", ...).await();
    return;
}

InventoryResultEvent invResult = inventoryEventTask.await(); // 既に完了済み → 即時返却
if (!invResult.success()) {
    // 在庫不足 → 補償して終了
    ctx.callActivity("PublishOrderCancelled", ...).await();
    return;
}
```

`anyOf` が返す `Task<?>` の **参照比較（`==`）** で winner を判定する。`inventoryEventTask.await()` は `anyOf` で既に完了済みのためブロックしない。

> **⚠️ Timer のキャンセルについて**
> `anyOf` で負けた Timer は自動キャンセルされない（SDK 仕様）。
> 正常フローでも 30 分間 Timer がアクティブ状態として DTS に残る。
> 詳細は [`durable-timer-behaviour.md`](./durable-timer-behaviour.md) を参照。

#### 補償フローの差異

| タイムアウト箇所 | 補償内容 | 理由 |
|---|---|---|
| 在庫引当タイムアウト | `PublishOrderCancelled` のみ | まだ在庫を引き当てていないため release 不要 |
| 配送タイムアウト | `PublishInventoryRelease` → `PublishOrderCancelled` | 在庫引当は成功済みのため必ず解放する |

---

### 3. Activity Functions — 副作用の実行

**ファイル**: [`services/saga-orchestrator/src/main/java/com/example/saga/ActivityFunctions.java`](../services/saga-orchestrator/src/main/java/com/example/saga/ActivityFunctions.java)

| Activity 名 | 処理内容 | 方向 |
|---|---|---|
| `CreateOrder` | OrderService に HTTP POST → orderId を取得 | HTTP OUT |
| `PublishInventoryReserveCommand` | `inventory.reserve.command` を Kafka 発行 | Kafka OUT |
| `PublishInventoryRelease` | `inventory.release.command` を Kafka 発行（補償） | Kafka OUT |
| `PublishShippingScheduleCommand` | `shipping.schedule.command` を Kafka 発行 | Kafka OUT |
| `PublishOrderConfirmed` | `order.confirmed` を Kafka 発行 | Kafka OUT |
| `PublishOrderCancelled` | `order.cancelled` を Kafka 発行（補償） | Kafka OUT |

I/O（HTTP 呼び出し・Kafka 送信）を Activity に隔離することで、オーケストレーターの Replay 安全性を確保している。Activity の戻り値・例外はすべて DTS の履歴に永続化される。

**Kafka プロデューサーの Singleton 化（Double-Checked Locking）**

```java
private static volatile KafkaProducer<String, String> kafkaProducer;

private static KafkaProducer<String, String> getKafkaProducer() {
    if (kafkaProducer == null) {
        synchronized (ActivityFunctions.class) {
            if (kafkaProducer == null) {
                kafkaProducer = new KafkaProducer<>(props);
            }
        }
    }
    return kafkaProducer;
}
```

Azure Functions のプロセス内で複数の Activity が並行起動するため、`KafkaProducer`（スレッドセーフ）を Singleton で共有している。

---

### 4. `KafkaConsumerFunctions` — イベントブリッジ（Kafka → Durable）

**ファイル**: [`services/saga-orchestrator/src/main/java/com/example/saga/KafkaConsumerFunctions.java`](../services/saga-orchestrator/src/main/java/com/example/saga/KafkaConsumerFunctions.java)

```java
@FunctionName("InventoryReservedConsumer")
public void inventoryReservedConsumer(
        @KafkaTrigger(topic = "inventory.reserved", ...)
        String message,
        @DurableClientInput(name = "durableClient")
        DurableClientContext durableContext, ...) throws Exception {

    InventoryReservedMsg msg = MAPPER.readValue(extractValue(message), ...);

    // Orchestrator の waitForExternalEvent("InventoryResult") を解除
    client.raiseEvent(msg.orchestrationId(), "InventoryResult",
                      new InventoryResultEvent(true, null));
}
```

#### 相関メカニズム（Orchestrator と Kafka の紐付け）

```
Orchestrator の instanceId
    │
    └─ InventoryReserveCommand に orchestrationId として埋め込んで Kafka 送信
              │
              └─ InventoryService がレスポンスに orchestrationId をそのまま返す
                          │
                          └─ KafkaConsumer が raiseEvent(orchestrationId, "InventoryResult", ...)
```

InventoryService・ShippingService は `orchestrationId` の意味を知らなくても、コマンドに含まれた値をレスポンスにそのまま返すだけでよい設計になっている。

#### `extractValue` の役割

```java
private static String extractValue(String raw) throws Exception {
    JsonNode node = MAPPER.readTree(raw);
    // KafkaTrigger が {"Offset":..., "Value":"<実際のJSON>"} 形式でラップして渡す
    for (String key : new String[]{"Value", "value"}) {
        JsonNode valueNode = node.get(key);
        if (valueNode != null) {
            return valueNode.isTextual() ? valueNode.asText() : valueNode.toString();
        }
    }
    return raw;
}
```

`KafkaTrigger` は `cardinality=ONE` かつ `dataType="string"` のとき、メッセージ本体を `{"Offset":..., "Value":"..."}` という Kafka メタデータごとラップして渡す。`extractValue` で `Value` フィールドだけを取り出すことで実際のビジネスメッセージを得ている。

---

## 正常フローのシーケンス図

```
クライアント  StartOrderSaga  Orchestrator  ActivityFunctions  OrderService  InventoryService  ShippingService  KafkaConsumer
    │               │               │               │               │               │               │               │
    │─POST /orders─▶│               │               │               │               │               │               │
    │               │─scheduleNew──▶│               │               │               │               │               │
    │◀─202+statusUri│               │               │               │               │               │               │
    │               │               │─callActivity─▶│ CreateOrder   │               │               │               │
    │               │               │               │──HTTP POST───▶│               │               │               │
    │               │               │               │◀─orderId──────│               │               │               │
    │               │               │◀─orderId──────│               │               │               │               │
    │               │               │─callActivity─▶│ PublishInventoryReserveCommand │               │               │
    │               │               │               │──reserve.cmd─────────────────▶│               │               │
    │               │               │─waitForEvent──│ (一時停止)                     │               │               │
    │               │               │               │               │    処理完了    │               │               │
    │               │               │               │               │               │──inventory.reserved──────────▶│
    │               │               │               │               │               │               │InventoryReservedConsumer
    │               │               │◀──────────────────────────────────────────────────────────────raiseEvent("InventoryResult")
    │               │               │ (Replay)       │               │               │               │               │
    │               │               │─callActivity─▶│ PublishShippingScheduleCommand │               │               │
    │               │               │               │──schedule.cmd──────────────────────────────▶│               │
    │               │               │─waitForEvent──│ (一時停止)                                    │               │
    │               │               │               │               │               │  処理完了     │               │
    │               │               │               │               │               │               │──shipping.scheduled──▶│
    │               │               │               │               │               │               │ ShippingScheduledConsumer
    │               │               │◀──────────────────────────────────────────────────────────────────raiseEvent("ShippingResult")
    │               │               │ (Replay)       │               │               │               │               │
    │               │               │─callActivity─▶│ PublishOrderConfirmed         │               │               │
    │               │               │               │──order.confirmed─────────────▶│               │               │
    │               │               │◀─done─────────│               │               │               │               │
    │─GET statusUri▶│               │               │               │               │               │               │
    │◀─Completed────│               │               │               │               │               │               │
```

### 列の実体と Durable Functions の役割

| 列名 | 実体 | Durable Functions の役割 |
|---|---|---|
| **StartOrderSaga** | `HttpTriggerFunction.java` | **Client** — Orchestrator を起動する |
| **Orchestrator** | `SagaOrchestratorFunction.java` | **Orchestrator Function** — フロー全体を定義する |
| **ActivityFunctions** | `ActivityFunctions.java` | **Activity Function** — 各ステップの I/O を実行する。Durable Functions は「Orchestrator が直接 I/O してはいけない（Replay 制約）」という仕様があるため、I/O を担う専用の Function として Activity を定義する |
| **OrderService** | 別プロセス（Spring Boot） | 外部サービス |
| **InventoryService** | 別プロセス（Spring Boot） | 外部サービス |
| **ShippingService** | 別プロセス（Spring Boot） | 外部サービス |

Orchestrator は「何をする順番か」だけを定義し、「実際に何かする」のはすべて Activity に委譲する。

```
Orchestrator（フロー制御のみ・I/O 禁止）
    │
    └── callActivity("CreateOrder")
              │
              ▼
         Activity（HTTP/Kafka などの実際の I/O を実行）
```

`ActivityFunctions.java` 1 ファイルに全 Activity をまとめており、`@DurableActivityTrigger` が付いた 6 メソッドがそれぞれ独立した Function として登録されている。

```
ActivityFunctions.java
  ├── @FunctionName("CreateOrder")                    → HTTP POST to OrderService
  ├── @FunctionName("PublishInventoryReserveCommand") → Kafka 送信
  ├── @FunctionName("PublishInventoryRelease")        → Kafka 送信（補償）
  ├── @FunctionName("PublishShippingScheduleCommand") → Kafka 送信
  ├── @FunctionName("PublishOrderConfirmed")          → Kafka 送信
  └── @FunctionName("PublishOrderCancelled")          → Kafka 送信（補償）
```

### ワークフローの抽象パターン

`SagaOrchestratorFunction.java` のワークフロー実装は **3 つのパターン** で構成されている。

#### 全体の抽象構造

```java
orderId = Step("CreateOrder")

Step("PublishInventoryReserveCommand")
WaitAndBranch("InventoryResult") {
    タイムアウト → Compensate(キャンセルのみ) → return
    失敗       → Compensate(キャンセルのみ) → return
}

Step("PublishShippingScheduleCommand")
WaitAndBranch("ShippingResult") {
    タイムアウト → Compensate(在庫解放 → キャンセル) → return
}

Step("PublishOrderConfirmed")
// 正常完了
```

ワークフロー全体は **「Step → WaitAndBranch → Step → WaitAndBranch → Step」の直列チェーン**で、失敗したら各分岐点で `Compensate → return` するシンプルな構造になっている。

---

#### パターン 1: 実行して進む（Step）

```java
String orderId = ctx.callActivity("CreateOrder", input, String.class).await();
```

```
callActivity(名前, 入力) → .await() → 結果を受け取って次へ
```

Activity を呼び出し、完了するまで待つだけ。成功前提で次のステップへ進む。

#### パターン 2: 待って分岐する（Wait & Branch）

在庫待ちも配送待ちも同じ構造で書かれている。

```java
// コマンド発行
ctx.callActivity("PublishXxxCommand", cmd).await();

// 外部イベント と タイムアウト を競合させる
Task<XxxResultEvent> eventTask = ctx.waitForExternalEvent("XxxResult", ...);
Task<Void>           timerTask = ctx.createTimer(...plus(30分));
Task<?> winner = ctx.anyOf(eventTask, timerTask).await();

// どちらが先に来たか
if (winner == timerTask)          { /* タイムアウト → 補償 → return */ }
if (!eventTask.await().success()) { /* 失敗通知   → 補償 → return */ }
// 正常 → 次のステップへ
```

```
発行 → [タイムアウト or 外部イベント] → 分岐
            │                  │
         補償して終了          次のステップへ
```

#### パターン 3: 補償して終わる（Compensate & Return）

```java
// 在庫タイムアウト（まだ在庫を引き当てていない）
ctx.callActivity("PublishOrderCancelled", ...).await();
return;

// 配送タイムアウト（在庫は引き当て済み → 解放が必要）
ctx.callActivity("PublishInventoryRelease", ...).await();
ctx.callActivity("PublishOrderCancelled", ...).await();
return;
```

```
補償 Activity を順番に実行 → return（Completed で終了）
```

---

## 補償フローのシーケンス図（在庫不足）

```
Orchestrator        ActivityFunctions      InventoryService
    │                    │                      │
    │─callActivity───────▶ PublishInventoryReserveCommand
    │                    │──inventory.reserve.command──▶│
    │─waitForEvent───────│(一時停止)                    │
    │                    │           在庫不足を検出      │
    │                    │◀──inventory.reservation.failed│
    │◀─raiseEvent(success=false)
    │(Replay・再起動)
    │  invResult.success() == false
    │─callActivity───────▶ PublishOrderCancelled
    │                    │──order.cancelled──▶ (OrderService)
    │  return (Completed)
```

---

## 新しい外部サービスを追加する手順

InventoryService と ShippingService の間に **RecommendService**（おすすめ商品提案）を追加する場合を例に説明する。

### 必要な作業（4 つ）

| # | 作業対象 | 内容 |
|---|---|---|
| 1 | **RecommendService（新規）** | Spring Boot サービス本体。`recommend.command` を購読し、処理後に `recommend.completed` を発行する |
| 2 | **`ActivityFunctions.java`** | `PublishRecommendCommand` Activity を追加。`recommend.command` を Kafka 送信する |
| 3 | **`KafkaConsumerFunctions.java`** | `RecommendCompletedConsumer` を追加。`recommend.completed` を受信後に `raiseEvent("RecommendResult")` を呼ぶ |
| 4 | **`SagaOrchestratorFunction.java`** | InventoryResult 確認後・ShippingScheduleCommand 発行前に `callActivity` + `waitForExternalEvent` + タイムアウトを追加する |


### 変更イメージ

**`ActivityFunctions.java`** — Activity を 1 つ追加

```java
@FunctionName("PublishRecommendCommand")
public void publishRecommendCommand(
        @DurableActivityTrigger(name = "taskActivityContext")
        RecommendCommand cmd,
        ExecutionContext context) throws Exception {
    String json = MAPPER.writeValueAsString(cmd);
    sendKafka("recommend.command", cmd.orderId(), json);
}
```

**`KafkaConsumerFunctions.java`** — Kafka → Durable ブリッジを 1 つ追加

```java
@FunctionName("RecommendCompletedConsumer")
public void recommendCompletedConsumer(
        @KafkaTrigger(
                name = "kafkaTrigger",
                topic = "recommend.completed",
                brokerList = "%KAFKA_BOOTSTRAP_SERVERS%",
                consumerGroup = "saga-orchestrator",
                dataType = "string",
                cardinality = Cardinality.ONE)
        String message,
        @DurableClientInput(name = "durableClient")
        DurableClientContext durableContext,
        ExecutionContext context) throws Exception {

    RecommendCompletedMsg msg = MAPPER.readValue(extractValue(message), RecommendCompletedMsg.class);
    durableContext.getClient().raiseEvent(
            msg.orchestrationId(), "RecommendResult",
            new RecommendResultEvent(true, msg.recommendationId()));
}
```

**`SagaOrchestratorFunction.java`** — InventoryResult 確認後に挿入

```java
// （既存）在庫引当成功を確認した後 ─────────────────────────────
InventoryResultEvent invResult = inventoryEventTask.await();
if (!invResult.success()) { /* 補償 */ return; }

// ★ここから追加 ──────────────────────────────────────────────
RecommendCommand recCmd = new RecommendCommand(
        orderId, input.items(), instanceId, ctx.getCurrentInstant().toString());
ctx.callActivity("PublishRecommendCommand", recCmd).await();

Task<RecommendResultEvent> recommendEventTask =
        ctx.waitForExternalEvent("RecommendResult", RecommendResultEvent.class);
Task<Void> recommendTimerTask =
        ctx.createTimer(ctx.getCurrentInstant().atZone(ZoneOffset.UTC)
                .plus(Duration.ofMinutes(TIMEOUT_MINUTES)));

Task<?> recommendWinner = ctx.anyOf(recommendEventTask, recommendTimerTask).await();
if (recommendWinner == recommendTimerTask) {
    ctx.callActivity("PublishInventoryRelease", ...).await();
    ctx.callActivity("PublishOrderCancelled", ...).await();
    return;
}
// ★ここまで追加 ──────────────────────────────────────────────

// （既存）配送手配へ続く
ShippingScheduleCommand shpCmd = ...
```

### データフロー（追加後）

```
Orchestrator
    │
    ├── ① CreateOrder
    ├── ② PublishInventoryReserveCommand → inventory.reserve.command
    │        └── waitForExternalEvent("InventoryResult")
    │
    ├── ③ PublishRecommendCommand        → recommend.command          ★新規
    │        └── waitForExternalEvent("RecommendResult")              ★新規
    │
    ├── ④ PublishShippingScheduleCommand → shipping.schedule.command
    │        └── waitForExternalEvent("ShippingResult")
    │
    └── ⑤ PublishOrderConfirmed          → order.confirmed
```

---

## 関連ファイル

| ファイル | 役割 |
|---|---|
| [`HttpTriggerFunction.java`](../services/saga-orchestrator/src/main/java/com/example/saga/HttpTriggerFunction.java) | HTTP エントリポイント、Saga 起動 |
| [`SagaOrchestratorFunction.java`](../services/saga-orchestrator/src/main/java/com/example/saga/SagaOrchestratorFunction.java) | フロー制御（Orchestrator） |
| [`ActivityFunctions.java`](../services/saga-orchestrator/src/main/java/com/example/saga/ActivityFunctions.java) | 各ステップの I/O 実行 |
| [`KafkaConsumerFunctions.java`](../services/saga-orchestrator/src/main/java/com/example/saga/KafkaConsumerFunctions.java) | Kafka → Durable イベントブリッジ |
| [`durable-timer-behaviour.md`](./durable-timer-behaviour.md) | Timer のキャンセル仕様と注意点 |
| [`spec/e2e-test.ps1`](../spec/e2e-test.ps1) | E2E テストスクリプト |
