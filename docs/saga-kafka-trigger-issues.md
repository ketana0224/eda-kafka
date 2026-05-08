# Saga Orchestrator - 不具合まとめ

調査日: 2026-05-07

---

## 問題0: AzureWebJobsStorage の Storage Account がパブリックアクセス無効

### 症状

Function App の起動・実行時に以下のエラーが発生し、Durable Functions が動作しない。

```
AuthorizationFailure: This request is not authorized to perform this operation.
```

Orchestrator が一切起動しない、または `ExecutionStarted` が記録されない。

### 根本原因

`AzureWebJobsStorage` に指定されている Storage Account（`stketanaext2saga`、Japan East）の  
**パブリックネットワークアクセスが `Disabled`** に設定されていた。

Durable Functions は内部で Azure Storage（Blob・Queue・Table）を使用するため、  
Function App のマネージド ID またはサービスから Storage Account へのアクセスが必要。  
パブリックアクセスが無効かつプライベートエンドポイントも未設定の場合、接続が完全に遮断される。

### 対応

```bash
az storage account update \
  --name stketanaext2saga \
  --resource-group rg-ketana-ext2-eda-kafka \
  --public-network-access Enabled
```

> **注意**: 本番環境では Enabled にする代わりに、プライベートエンドポイントまたは  
> サービスエンドポイント + 信頼された Azure サービスの許可を使うことが推奨される。

### ⚠️ Azure Policy 制約による再現性

この環境では **Azure Policy により Storage Account のパブリックアクセスが自動的に `Disabled` に戻る**制約がかかっている。  
そのため、E2E シナリオを確認する際は **事前準備として毎回以下を実施する**こと。

```powershell
# E2E テスト実施前の事前準備
az storage account update `
  --name stketanaext2saga `
  --resource-group rg-ketana-ext2-eda-kafka `
  --public-network-access Enabled

# 設定反映を確認
az storage account show `
  --name stketanaext2saga `
  --resource-group rg-ketana-ext2-eda-kafka `
  --query "publicNetworkAccess" -o tsv
# → "Enabled" であることを確認してからテストを開始する
```

---

## 問題1: `KafkaTrigger` が Kafka メッセージをラッパー JSON で渡す

### 症状

`InventoryReservedConsumer` が以下の例外で失敗し続ける。

```
UnrecognizedPropertyException: Unrecognized field "Offset"
(class com.example.saga.model.InventoryReservedMsg)
at com.example.saga.KafkaConsumerFunctions.inventoryReservedConsumer(KafkaConsumerFunctions.java:46)
```

### 根本原因

Azure Functions の `KafkaTrigger` は `cardinality=ONE` + `dataType="string"` + `String` パラメータの場合、  
Kafka メッセージの **Value（ペイロード）をそのまま渡すのではなく**、以下のラッパー JSON 全体を文字列として渡す。

```json
{
  "Offset": 12345,
  "Partition": 0,
  "Topic": "inventory.reserved",
  "Timestamp": "2026-05-07T00:24:53Z",
  "Value": "{\"orderId\":\"...\",\"orchestrationId\":\"...\"}",
  "Headers": []
}
```

`Value` フィールドが実際のメッセージペイロード（エスケープ済みJSON文字列）。  
`MAPPER.readValue(message, InventoryReservedMsg.class)` を直接呼ぶと `"Offset"` フィールドが未知扱いになる。

### 対応

`extractValue()` メソッドで `Value` フィールドを取り出してから `readValue` する。

```java
private static String extractValue(String raw) throws Exception {
    JsonNode node = MAPPER.readTree(raw);
    for (String key : new String[]{"Value", "value"}) {
        JsonNode valueNode = node.get(key);
        if (valueNode != null) {
            return valueNode.isTextual() ? valueNode.asText() : valueNode.toString();
        }
    }
    return raw;
}
```

各 Consumer で `MAPPER.readValue(message, ...)` を  
`MAPPER.readValue(extractValue(message), ...)` に変更する。  
対象: `InventoryReservedConsumer`、`InventoryReservationFailedConsumer`、`ShippingScheduledConsumer`

---

## 問題2: `mvn azure-functions:package` が `cardinality` を function.json から削除する

### 症状

`@KafkaTrigger(cardinality = Cardinality.ONE)` をソースに記述しても、  
`mvn azure-functions:package` を実行するたびに生成される `function.json` から `"cardinality"` が消える。

```json
// 生成後（cardinality がない）
{
  "type": "kafkaTrigger",
  "brokerList": "%KAFKA_BOOTSTRAP_SERVERS%"
  // "cardinality": "ONE" が入らない
}
```

`cardinality` がない場合、デフォルトが `MANY`（バッチ）になり、  
`String` パラメータにバッチ配列が来てデシリアライズ失敗する。

### 根本原因

`azure-functions-maven-plugin` が `Cardinality.ONE` アノテーションの function.json 出力に対応していない（バグ/未実装）。

### 対応（暫定）

`mvn azure-functions:package` 実行後、毎回手動で 3 ファイルに追記する。

```
target/azure-functions/func-ketana-ext2-saga-orch/InventoryReservedConsumer/function.json
target/azure-functions/func-ketana-ext2-saga-orch/InventoryReservationFailedConsumer/function.json
target/azure-functions/func-ketana-ext2-saga-orch/ShippingScheduledConsumer/function.json
```

各ファイルの `"brokerList"` 行の後に以下を追加する。

```json
"cardinality": "ONE"
```

> **注意**: `mvn azure-functions:deploy` は再 package しないため、  
> `deploy` のみ実行する場合は追加済みの状態が維持される。

---

## 問題3: `mvn compile` + `mvn azure-functions:package` では JAR が更新されない

### 症状

ソースを修正して `mvn compile` → `mvn azure-functions:package` → `mvn azure-functions:deploy` を実行しても、  
旧コードの動作が継続する（`extractValue` が含まれない旧 JAR がデプロイされる）。

### 根本原因

- `mvn compile` はクラスファイル（`.class`）を更新するが、  
  JAR ファイル（`target/saga-orchestrator-1.0-SNAPSHOT.jar`）は更新しない。  
- `mvn azure-functions:package` は既存の JAR を `target/azure-functions/` にコピーするだけで、  
  JAR の再作成は行わない。

### 対応

ソース変更後は必ず以下のシーケンスで実行する。

```powershell
# 1. JAR を再ビルド（これで extractValue が含まれる）
& "C:\tools\maven\apache-maven-3.9.6\bin\mvn.cmd" package -DskipTests

# 2. function.json に cardinality: ONE を手動追加（package で再生成されるため）
#    （InventoryReservedConsumer, InventoryReservationFailedConsumer, ShippingScheduledConsumer）

# 3. デプロイ（再 package しない）
& "C:\tools\maven\apache-maven-3.9.6\bin\mvn.cmd" azure-functions:deploy
```

---

## 正しいビルド・デプロイ手順（まとめ）

```powershell
cd C:\GitHub\eda-kafka\services\saga-orchestrator

# Step 1: JAR 再ビルド（compile + jar 作成 + azure-functions:package）
& "C:\tools\maven\apache-maven-3.9.6\bin\mvn.cmd" package -DskipTests

# Step 2: 3 つの function.json に cardinality: ONE を追加
$funcBase = "target\azure-functions\func-ketana-ext2-saga-orch"
foreach ($fn in @("InventoryReservedConsumer","InventoryReservationFailedConsumer","ShippingScheduledConsumer")) {
    $path = "$funcBase\$fn\function.json"
    $content = Get-Content $path -Raw
    if ($content -notmatch '"cardinality"') {
        $content = $content -replace '("brokerList"\s*:\s*"%KAFKA_BOOTSTRAP_SERVERS%")', '$1,`n    "cardinality" : "ONE"'
        Set-Content $path $content
    }
}

# Step 3: デプロイ
& "C:\tools\maven\apache-maven-3.9.6\bin\mvn.cmd" azure-functions:deploy
```

---

## まとめ表

| # | 問題 | 根本原因 | 対応 |
|---|------|----------|------|
| 0 | Orchestrator が全く動かない（AuthorizationFailure） | `stketanaext2saga`（AzureWebJobsStorage）のパブリックアクセスが `Disabled` | `az storage account update --public-network-access Enabled` |
| 1 | `UnrecognizedPropertyException: "Offset"` | KafkaTrigger がペイロードをラッパー JSON `{"Offset":..., "Value":"..."}` で渡す | `extractValue()` で `Value` を取り出してから `readValue` |
| 2 | package するたびに `cardinality` が消える | `azure-functions-maven-plugin` が `Cardinality.ONE` を function.json に出力しない（バグ） | 毎回手動追加。`deploy` のみなら維持される |
| 3 | `mvn compile` + `azure-functions:package` で旧コードがデプロイされる | `compile` は .class 更新のみ、JAR は更新しない | 必ず `mvn package -DskipTests` で JAR 再作成してからデプロイ |

---

## 関連環境情報

| 項目 | 値 |
|------|-----|
| Function App | `func-ketana-ext2-saga-orch` |
| Extension Bundle | `[4.*, 5.0.0)` v4.33.1 |
| Java | 21 |
| Kafka Broker | `20.210.80.7:9092` (ACI) |
| Maven | `C:\tools\maven\apache-maven-3.9.6\bin\mvn.cmd` |
