# EDA Orchestration Saga の検証計画

## 1. 目的
C:\GitHub\eda-kafka\docs\eda_plan.mdの結果を踏まえ、Orchestration Sagaの検証を実施する
検証環境は Azure を利用する
Kafkaは既に C:\GitHub\eda-kafka\spec\1.kafka.md で構築済 

## 2. 環境計画
既存の Kafka に対して Orchestration Saga 検証用の Azure サービスを計画する
durable functions になると考えている　https://learn.microsoft.com/en-us/azure/architecture/patterns/saga
Durable Task Monitor による監視も行う
C:\GitHub\eda-kafka\docs\eda_plan.md 3. 検証システム要件 が検証システム要件になる

## 3. 検証環境の作成

### 3.1 アーキテクチャ概要

Orchestration Saga の指揮者として **Azure Durable Functions** を採用する。  
各マイクロサービスは既存の **Azure Container Apps 環境**にデプロイし、既存 Kafka（ACI）とイベントで連携する。

```
[HTTP Client]
     │ POST /orders
     ▼
[OrderService (ACA)]──────────────────────────────────────────────────────────────┐
     │ Kafka: order.created                                                        │ order.confirmed
     ▼                                                                             │ order.cancelled
[Durable Functions Orchestrator (func-ketana-ext2-eda-saga)]                      │
     │                                                                             │
     ├─▶ Kafka: inventory.reserve.command                                          │
     │         ▼                                                                   │
     │   [InventoryService (ACA)] ──▶ PostgreSQL(inventory)               │
     │         │ Kafka: inventory.reserved                                         │
     │         │       inventory.reservation.failed                                │
     │         ▼                                                                   │
     ├─◀ WaitForExternalEvent ──────────────────────────────────────────────────── │
     │                                                                             │
     ├─▶ Kafka: shipping.schedule.command                                          │
     │         ▼                                                                   │
     │   [ShippingService (ACA)] ──▶ PostgreSQL(shipping)                         │
     │         │ Kafka: shipping.scheduled                                         │
     │         ▼                                                                   │
     └─◀ WaitForExternalEvent ──────────────────────────────────────────────────▶─┘

補償フロー（在庫不足時）:
  Orchestrator ──▶ Kafka: inventory.release.command ──▶ InventoryService
  Orchestrator ──▶ Kafka: order.cancelled ──▶ OrderService
```

Durable Functions の `WaitForExternalEvent` で Kafka 応答イベントを待機し、`CreateTimer` でタイムアウト自動キャンセルを実現する。

---

### 3.2 リソース一覧

#### 新規作成リソース

| リソース種別 | リソース名 | 用途 |
|---|---|---|
| Log Analytics Workspace | `log-ketana-ext2-eda-kafka` | ログ集約 |
| Application Insights | `appi-ketana-ext2-eda-kafka` | 分散トレーシング・監視 |
| Storage Account | `stketanaext2saga` | Durable Functions 状態ストア |
| Function App | `func-ketana-ext2-eda-saga` | Orchestration Saga 指揮者（Durable Functions） |
| PostgreSQL Flexible Server | `psql-ketana-ext2-order` | OrderService 専用 DB |
| PostgreSQL Flexible Server | `psql-ketana-ext2-inventory` | InventoryService 専用 DB |
| PostgreSQL Flexible Server | `psql-ketana-ext2-shipping` | ShippingService 専用 DB |
| Container App | `aca-order` | OrderService |
| Container App | `aca-inventory` | InventoryService |
| Container App | `aca-shipping` | ShippingService |
| Container App | `aca-dtm` | Durable Task Monitor（Saga 監視 UI） |

#### 既存リソース（変更なし・流用）

| リソース種別 | リソース名 | 備考 |
|---|---|---|
| Resource Group | `rg-ketana-ext2-eda-kafka` | 既存 RG に新規リソースを追加 |
| Container Group (ACI) | `ci-ketana-ext2-eda-kafka` | 既存 Kafka（公開 IP） |
| ACA Environment | `aca-env-ketana-eda-kafka` | 既存 ACA 環境を再利用 |

---

### 3.3 Kafka トピック設計

| トピック名 | パブリッシャー | サブスクライバー | 用途 |
|---|---|---|---|
| `order.created` | OrderService | Orchestrator | 注文受付イベント（Saga 起動トリガー） |
| `inventory.reserve.command` | Orchestrator | InventoryService | 在庫引当コマンド |
| `inventory.reserved` | InventoryService | Orchestrator | 在庫引当成功レスポンス |
| `inventory.reservation.failed` | InventoryService | Orchestrator | 在庫不足レスポンス |
| `inventory.release.command` | Orchestrator | InventoryService | 引当解放コマンド（補償） |
| `shipping.schedule.command` | Orchestrator | ShippingService | 配送手配コマンド |
| `shipping.scheduled` | ShippingService | Orchestrator | 配送手配完了レスポンス |
| `order.confirmed` | Orchestrator | OrderService | 注文確定通知 |
| `order.cancelled` | Orchestrator | OrderService | 注文キャンセル通知（補償） |

各トピック: パーティション数 3、レプリケーション係数 1（単一ブローカーのため）

---

### 3.4 前提条件

- `spec/1.kafka.md` の手順で Kafka（ACI: `ci-ketana-ext2-eda-kafka`）がデプロイ済みで `Running` 状態であること
- ACA 環境 `aca-env-ketana-eda-kafka` が作成済みであること
- Azure Functions Core Tools v4 がインストール済みであること
- 各マイクロサービスのコンテナイメージが ACR またはコンテナレジストリに push 済みであること

---

### 3.5 デプロイ手順

#### Phase 1 — 前提条件確認

```powershell
$cfg = Import-PowerShellDataFile .\infra\config.psd1
az account set --subscription $cfg.SubscriptionId

# Kafka ACI 稼働確認
az container show -n $cfg.ContainerGroup -g $cfg.ResourceGroup `
  --query instanceView.state -o tsv
# 期待値: Running

# Kafka 公開 IP 取得（以降のフェーズで使用）
$kafkaIp = az container show -n $cfg.ContainerGroup -g $cfg.ResourceGroup `
  --query ipAddress.ip -o tsv

# ACA 環境確認
az containerapp env show -n $cfg.AcaEnvironment -g $cfg.ResourceGroup `
  --query properties.provisioningState -o tsv
# 期待値: Succeeded
```

#### Phase 2 — 監視基盤の作成

```powershell
# Log Analytics Workspace
az monitor log-analytics workspace create `
  --resource-group $cfg.ResourceGroup `
  --workspace-name "log-ketana-ext2-eda-kafka" `
  --location $cfg.Location

$lawId = az monitor log-analytics workspace show `
  -g $cfg.ResourceGroup -n "log-ketana-ext2-eda-kafka" `
  --query id -o tsv

# Application Insights
az monitor app-insights component create `
  --app "appi-ketana-ext2-eda-kafka" `
  --location $cfg.Location `
  --resource-group $cfg.ResourceGroup `
  --workspace $lawId

$appInsightsKey = az monitor app-insights component show `
  --app "appi-ketana-ext2-eda-kafka" -g $cfg.ResourceGroup `
  --query instrumentationKey -o tsv
```

#### Phase 3 — データストアの作成

```powershell
# PostgreSQL Flexible Server × 3（各サービス専用）
$pgServices = @(
  @{ Name = "psql-ketana-ext2-order";     Db = "orderdb"     },
  @{ Name = "psql-ketana-ext2-inventory"; Db = "inventorydb" },
  @{ Name = "psql-ketana-ext2-shipping";  Db = "shippingdb"  }
)

foreach ($pg in $pgServices) {
  az postgres flexible-server create `
    --resource-group $cfg.ResourceGroup `
    --name $pg.Name `
    --location $cfg.Location `
    --admin-user sagaadmin `
    --admin-password "<SECURE_PASSWORD>" `
    --sku-name Standard_B1ms `
    --tier Burstable `
    --public-access 0.0.0.0 `
    --database-name $pg.Db
}

# Azure Cache for Redis（InventoryService 在庫読取キャッシュ）
az redis create `
  --resource-group $cfg.ResourceGroup `
  --name "redis-ketana-ext2-inventory" `
  --location $cfg.Location `
  --sku Basic `
  --vm-size C0
```

> **⚠️ 注意**: `--public-access 0.0.0.0` は検証環境向け設定（全 IP からのアクセスを許可）。本番環境では VNet 統合またはプライベートエンドポイントを使用すること。

#### Phase 4 — Orchestrator (Durable Functions) の作成

```powershell
# Storage Account（Durable Functions 状態ストア）
az storage account create `
  --name "stketanaext2saga" `
  --resource-group $cfg.ResourceGroup `
  --location $cfg.Location `
  --sku Standard_LRS

# Function App（Java 21 / Durable Functions v4）
az functionapp create `
  --resource-group $cfg.ResourceGroup `
  --consumption-plan-location $cfg.Location `
  --runtime java `
  --runtime-version 21 `
  --functions-version 4 `
  --name "func-ketana-ext2-eda-saga" `
  --storage-account "stketanaext2saga" `
  --app-insights-key $appInsightsKey

# アプリ設定（Kafka・DB 接続情報）
az functionapp config appsettings set `
  --name "func-ketana-ext2-eda-saga" `
  --resource-group $cfg.ResourceGroup `
  --settings `
    "KAFKA_BOOTSTRAP_SERVERS=${kafkaIp}:9092" `
    "ORDER_DB_URL=jdbc:postgresql://psql-ketana-ext2-order.postgres.database.azure.com/orderdb" `
    "FUNCTIONS_WORKER_RUNTIME=java" `
    "SAGA_TIMEOUT_MINUTES=30"
```

#### Phase 5 — マイクロサービス (Container Apps) のデプロイ

```powershell
# OrderService
az containerapp create `
  --name "aca-order" `
  --resource-group $cfg.ResourceGroup `
  --environment $cfg.AcaEnvironment `
  --image "<REGISTRY>/order-service:latest" `
  --target-port 8080 `
  --ingress external `
  --env-vars `
    "KAFKA_BOOTSTRAP_SERVERS=${kafkaIp}:9092" `
    "DB_URL=jdbc:postgresql://psql-ketana-ext2-order.postgres.database.azure.com/orderdb" `
    "APPLICATIONINSIGHTS_CONNECTION_STRING=$(az monitor app-insights component show --app appi-ketana-ext2-eda-kafka -g $cfg.ResourceGroup --query connectionString -o tsv)"

# InventoryService
az containerapp create `
  --name "aca-inventory" `
  --resource-group $cfg.ResourceGroup `
  --environment $cfg.AcaEnvironment `
  --image "<REGISTRY>/inventory-service:latest" `
  --target-port 8080 `
  --ingress internal `
  --env-vars `
    "KAFKA_BOOTSTRAP_SERVERS=${kafkaIp}:9092" `
    "DB_URL=jdbc:postgresql://psql-ketana-ext2-inventory.postgres.database.azure.com/inventorydb" `
    "REDIS_HOST=redis-ketana-ext2-inventory.redis.cache.windows.net"

# ShippingService
az containerapp create `
  --name "aca-shipping" `
  --resource-group $cfg.ResourceGroup `
  --environment $cfg.AcaEnvironment `
  --image "<REGISTRY>/shipping-service:latest" `
  --target-port 8080 `
  --ingress internal `
  --env-vars `
    "KAFKA_BOOTSTRAP_SERVERS=${kafkaIp}:9092" `
    "DB_URL=jdbc:postgresql://psql-ketana-ext2-shipping.postgres.database.azure.com/shippingdb"
```

#### Phase 6 — Kafka トピックの作成

```powershell
$topics = @(
  "order.created",
  "inventory.reserve.command",
  "inventory.reserved",
  "inventory.reservation.failed",
  "inventory.release.command",
  "shipping.schedule.command",
  "shipping.scheduled",
  "order.confirmed",
  "order.cancelled"
)

foreach ($topic in $topics) {
  az container exec `
    -n $cfg.ContainerGroup `
    -g $cfg.ResourceGroup `
    --exec-command "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $topic --partitions 3 --replication-factor 1"
}

# 作成確認
az container exec `
  -n $cfg.ContainerGroup `
  -g $cfg.ResourceGroup `
  --exec-command "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
```

#### Phase 7 — Durable Task Monitor のデプロイ

Durable Task Monitor は Durable Functions の Orchestration インスタンスの状態・履歴・実行ログをリアルタイムで確認できる OSS 監視 UI。  
参照: https://github.com/scale-tone/DurableTaskMonitor

```powershell
$storageConn = az storage account show-connection-string `
  -n "stketanaext2saga" -g $cfg.ResourceGroup --query connectionString -o tsv

az containerapp create `
  --name "aca-dtm" `
  --resource-group $cfg.ResourceGroup `
  --environment $cfg.AcaEnvironment `
  --image "ghcr.io/scale-tone/durable-task-monitor:latest" `
  --target-port 7072 `
  --ingress external `
  --env-vars "AzureWebJobsStorage=${storageConn}"

# Durable Task Monitor URL の取得
az containerapp show -n "aca-dtm" -g $cfg.ResourceGroup `
  --query properties.configuration.ingress.fqdn -o tsv
```

---

### 3.6 動作確認

| # | 確認内容 | 期待値 |
|---|---|---|
| 1 | Function App 稼働 | `az functionapp show -n func-ketana-ext2-eda-saga -g rg-ketana-ext2-eda-kafka --query state` → `Running` |
| 2 | Container Apps 稼働 | `aca-order` / `aca-inventory` / `aca-shipping` の `runningStatus` → `Running` |
| 3 | PostgreSQL 接続 | 各サービスのヘルスエンドポイントで DB 接続確認 |
| 4 | Redis 接続 | InventoryService ヘルスエンドポイントで Redis PING 確認 |
| 5 | Kafka トピック | 9 トピックが `kafka-topics.sh --list` で確認できること |
| 6 | Saga 正常フロー | `POST /orders` を OrderService に送信 → Durable Task Monitor で `Completed` 状態になること |
| 7 | 補償フロー | 在庫不足シナリオで注文が `CANCELLED` になり、Orchestrator が `Completed`（補償完了）になること |
| 8 | タイムアウト | InventoryService を停止した状態で注文送信 → 30 分後に自動キャンセルされること |
