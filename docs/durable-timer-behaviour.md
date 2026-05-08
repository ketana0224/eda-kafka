# Durable Timer の動作仕様

## 現象

シナリオ 10（正常フロー）実行後、DTS の Timeline に **Timer アクティビティ（オレンジ棒）が 30 分間動き続ける**。

![Timeline の例](images/scenario10-timeline.png)

---

## 原因

### コード上の構造

`SagaOrchestratorFunction.java` では `waitForExternalEvent` とタイムアウトタイマーを `anyOf` で競合させている。

```java
Task<InventoryResultEvent> inventoryEventTask =
        ctx.waitForExternalEvent("InventoryResult", InventoryResultEvent.class);
Task<Void> inventoryTimerTask =
        ctx.createTimer(ctx.getCurrentInstant().atZone(ZoneOffset.UTC)
                .plus(Duration.ofMinutes(TIMEOUT_MINUTES)));   // デフォルト 30 分

Task<?> inventoryWinner = ctx.anyOf(inventoryEventTask, inventoryTimerTask).await();
```

同様のパターンが **配送待ち（ShippingResult）** にも存在する。

### SDK の仕様

✅ **事実**：`com.microsoft.durabletask` SDK（バージョン 1.5.1）では、`ctx.anyOf()` で負けた側のタスクは **自動キャンセルされない**。

`createTimer` で生成された Durable Timer は Azure DTS（Durable Task Service）側に永続化されており、明示的にキャンセルしない限り **指定期限（30 分後）が来るまでアクティブ状態**のままとなる。

現バージョン（1.5.1）では `cancelTimer()` のような公開 API は存在しない。

---

## 影響範囲

| 項目 | 内容 |
|---|---|
| **オーケストレーションの正確性** | 影響なし。`anyOf` の勝者判定は正しく動作しており、期限到達時の分岐も通過済み |
| **DTS 上の表示** | Orchestration が Completed になった後も Timer がアクティブ表示される |
| **リソースコスト** | DTS の Timer 処理は軽量。30 分後に Timer が発火するが、Orchestration はすでに完了しているため何も実行されない |
| **再起動・再実行** | 影響なし |

---

## 対処オプション

### オプション 1: dev/test 環境でタイムアウトを短縮（推奨）

`local.settings.json` または Azure Functions の Application Settings で設定する。

```json
{
  "Values": {
    "SAGA_TIMEOUT_MINUTES": "1"
  }
}
```

これにより Timeline 上のオレンジ棒が 1 分後に消える。

### オプション 2: 現状維持（許容）

正常フローの業務ロジックに影響がないため、表示上の問題として許容する。

### オプション 3: SDK バージョンアップ

将来の `com.microsoft.durabletask` SDK でタイマーキャンセル API が追加された場合、以下のパターンで実装できる（現時点では未実装のため参考）。

```java
// 将来的な実装イメージ（現 SDK では不可）
if (inventoryWinner == inventoryEventTask) {
    inventoryTimerTask.cancel();  // API 未実装
}
```

---

## 関連ファイル

- [`services/saga-orchestrator/src/main/java/com/example/saga/SagaOrchestratorFunction.java`](../services/saga-orchestrator/src/main/java/com/example/saga/SagaOrchestratorFunction.java)
- [`spec/e2e-test.ps1`](../spec/e2e-test.ps1) — `Test-Scenario10` 定義
