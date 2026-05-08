<#
.SYNOPSIS
  Logic Apps Consumption — Saga Orchestrator デプロイ手順書
  実行者: 人間（手順通りに各フェーズを実行すること）

.NOTES
  前提条件:
    - az CLI ログイン済み (az login)
    - 対象サブスクリプションを選択済み
    - このスクリプトは C:\GitHub\eda-kafka から実行する

  制約:
    - allowSharedKeyAccess: false ポリシーが適用されているため
      Shared Key 認証を使う Managed Connector は使用不可
#   - Kafka: Azure Functions (KafkaPublisherFunction / KafkaConsumerFunctions) をブリッジとして使用
    - ストレージ: Cosmos DB for NoSQL + MSI で代替
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ── 設定 ────────────────────────────────────────────────────────
$cfg = Import-PowerShellDataFile "$PSScriptRoot\..\..\infra\config.psd1"

$ResourceGroup       = $cfg.ResourceGroup        # rg-ketana-ext2-eda-kafka  (既存サービス用)
$LogicAppRG          = 'rg-ketana-ext2-logicapp'  # Logic Apps 専用 RG
$Location            = $cfg.Location             # japaneast
$SubscriptionId      = $cfg.SubscriptionId

$WorkflowsDir        = "$PSScriptRoot\workflows"

# ★ 以下を実際の値に変更してから実行すること
$KafkaBootstrapServers = ""  # Phase 0 で自動設定される
$OrderServiceUrl       = "https://YOUR_ORDER_SERVICE_URL"

# ── フェーズ 0: 前提確認 ────────────────────────────────────────
Write-Host "`n=== Phase 0: 前提確認 ===" -ForegroundColor Cyan

# サブスクリプション確認
$currentSub = az account show --query "id" -o tsv
if ($currentSub -ne $SubscriptionId) {
    Write-Warning "サブスクリプションが異なります。切り替えます: $SubscriptionId"
    az account set --subscription $SubscriptionId
}
Write-Host "  Subscription: $SubscriptionId" -ForegroundColor Green

# 既存 RG（Kafka / ACA 等）の確認
$rgExists = az group exists --name $ResourceGroup
if ($rgExists -ne 'true') {
    Write-Error "既存リソースグループが存在しません: $ResourceGroup"
    exit 1
}
Write-Host "  ResourceGroup (existing): $ResourceGroup (OK)" -ForegroundColor Green

# Logic Apps 専用 RG を作成（なければ）
$laRgExists = az group exists --name $LogicAppRG
if ($laRgExists -ne 'true') {
    Write-Host "  Logic Apps RG を作成します: $LogicAppRG" -ForegroundColor Yellow
    az group create --name $LogicAppRG --location $Location | Out-Null
}
Write-Host "  ResourceGroup (logicapp): $LogicAppRG (OK)" -ForegroundColor Green

# Kafka ACI の公開 IP を取得して KafkaBootstrapServers を設定
Write-Host "`n  Kafka ACI IP を取得中..."
$kafkaIp = az container show -n $cfg.ContainerGroup -g $ResourceGroup `
    --query ipAddress.ip -o tsv 2>$null

if ([string]::IsNullOrEmpty($kafkaIp)) {
    Write-Error "Kafka ACI ($($cfg.ContainerGroup)) の IP を取得できませんでした。ACI が Running 状態か確認してください。"
    exit 1
}
$KafkaBootstrapServers = "${kafkaIp}:9092"
Write-Host "  KafkaBootstrapServers: $KafkaBootstrapServers" -ForegroundColor Green

# ── フェーズ 1: インフラデプロイ (az cli) ──────────────────────
Write-Host "`n=== Phase 1: インフラデプロイ ===" -ForegroundColor Cyan

$cosmosName        = 'cosmos-ketana-ext2-saga'
$bridgeStorageName = 'stketanaext2bridge'
$bridgePlanName    = 'asp-ketana-ext2-bridge'
$kafkaBridgeName   = 'func-ketana-ext2-kafka-bridge'
$callbackRegName   = 'la-ketana-ext2-callback-reg'
$orderSagaName     = 'la-ketana-ext2-order-saga'
$invReservedName   = 'la-ketana-ext2-inv-reserved'
$invFailedName     = 'la-ketana-ext2-inv-failed'
$shipSchedName     = 'la-ketana-ext2-ship-sched'
$KafkaBridgeUrl    = "https://$kafkaBridgeName.azurewebsites.net/api/kafka/publish"
$LaApiVersion      = '2019-05-01'
$LaBaseUri         = "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$LogicAppRG/providers/Microsoft.Logic/workflows"

# 1a. Cosmos DB ─────────────────────────────────────────────────
Write-Host "  [1a] Cosmos DB 作成中..."
az cosmosdb create -g $LogicAppRG -n $cosmosName `
    --kind GlobalDocumentDB `
    --default-consistency-level Session `
    --locations regionName=$Location failoverPriority=0 isZoneRedundant=false `
    --disable-local-auth false | Out-Null
az cosmosdb sql database create -g $LogicAppRG -a $cosmosName -n sagadb --throughput 400 | Out-Null
az cosmosdb sql container create -g $LogicAppRG -a $cosmosName -d sagadb -n CallbackUrls `
    --partition-key-path /orchestrationId --ttl 7200 | Out-Null

$cosmosEndpoint = az cosmosdb show -g $LogicAppRG -n $cosmosName --query documentEndpoint -o tsv
Write-Host "  Cosmos DB endpoint: $cosmosEndpoint" -ForegroundColor Green

# 1b. Storage Account (一時的に shared key 有効で作成、後で無効化) ────
# ⚠️ App Service Plan (Linux B1) は japaneast クォータ制限により japanwest で作成する
$FuncLocation = 'japanwest'
Write-Host "  [1b] Storage Account 作成中..."
az storage account create -g $LogicAppRG -n $bridgeStorageName `
    --location $FuncLocation `
    --sku Standard_LRS --kind StorageV2 `
    --https-only true --min-tls-version TLS1_2 | Out-Null

# 1c. App Service Plan + Function App ───────────────────────────
Write-Host "  [1c] App Service Plan / Function App 作成中..."
az appservice plan create -g $LogicAppRG -n $bridgePlanName `
    --location $FuncLocation --sku B1 --is-linux | Out-Null
az functionapp create -g $LogicAppRG -n $kafkaBridgeName `
    --plan $bridgePlanName `
    --runtime java --runtime-version 21.0 --functions-version 4 --os-type linux `
    --storage-account $bridgeStorageName | Out-Null

# MSI 有効化
az functionapp identity assign -g $LogicAppRG -n $kafkaBridgeName --identities '[system]' | Out-Null

# MSI ベースのストレージ設定に上書き（接続文字列を削除してアカウント名方式へ）
az functionapp config appsettings set -g $LogicAppRG -n $kafkaBridgeName --settings `
    "AzureWebJobsStorage__accountName=$bridgeStorageName" `
    "AzureWebJobsStorage__credential=managedidentity" `
    "KAFKA_BOOTSTRAP_SERVERS=$KafkaBootstrapServers" `
    "LOGIC_APP_INV_RESERVED_URL=" `
    "LOGIC_APP_INV_FAILED_URL=" `
    "LOGIC_APP_SHIP_SCHED_URL=" | Out-Null
az functionapp config appsettings delete -g $LogicAppRG -n $kafkaBridgeName `
    --setting-names AzureWebJobsStorage | Out-Null

# Shared Key を無効化
az storage account update -g $LogicAppRG -n $bridgeStorageName `
    --allow-shared-key-access false | Out-Null
Write-Host "  Function App 作成完了: $kafkaBridgeName" -ForegroundColor Green

# 1d. Storage RBAC ロール付与 ────────────────────────────────────
Write-Host "  [1d] Storage RBAC 付与中..."
$bridgePrincipalId = az functionapp identity show -g $LogicAppRG -n $kafkaBridgeName `
    --query principalId -o tsv
$storageScope = "/subscriptions/$SubscriptionId/resourceGroups/$LogicAppRG/providers/Microsoft.Storage/storageAccounts/$bridgeStorageName"
az role assignment create --role 'Storage Blob Data Owner' `
    --assignee-object-id $bridgePrincipalId --assignee-principal-type ServicePrincipal `
    --scope $storageScope | Out-Null
az role assignment create --role 'Storage Queue Data Contributor' `
    --assignee-object-id $bridgePrincipalId --assignee-principal-type ServicePrincipal `
    --scope $storageScope | Out-Null
az role assignment create --role 'Storage Table Data Contributor' `
    --assignee-object-id $bridgePrincipalId --assignee-principal-type ServicePrincipal `
    --scope $storageScope | Out-Null
Write-Host "  Storage RBAC 付与完了" -ForegroundColor Green

# 1e. Logic App デプロイ ─────────────────────────────────────────
Write-Host "  [1e] Logic Apps デプロイ中..."

# Deploy-LogicApp: JSON を一時ファイル経由で az rest に渡す（Windows PowerShell の引用符問題を回避）
function Deploy-LogicApp([string]$Name, [string]$DefFile, [hashtable]$Params) {
    $def  = Get-Content $DefFile -Raw | ConvertFrom-Json
    $body = @{
        location   = $Location
        identity   = @{ type = 'SystemAssigned' }
        properties = @{
            state      = 'Enabled'
            definition = $def
            parameters = $Params
        }
    } | ConvertTo-Json -Depth 30 -Compress
    $tmpFile = [System.IO.Path]::GetTempFileName()
    $body | Set-Content $tmpFile -Encoding utf8
    az rest --method PUT `
        --uri "${LaBaseUri}/${Name}?api-version=$LaApiVersion" `
        --body "@$tmpFile" `
        --headers 'Content-Type=application/json' | Out-Null
    Remove-Item $tmpFile -Force
    Write-Host "    $Name deployed" -ForegroundColor DarkGray
}

Deploy-LogicApp $callbackRegName "$WorkflowsDir\callback-reg.json" @{
    cosmosDbEndpoint = @{ value = $cosmosEndpoint }
}
Deploy-LogicApp $invReservedName "$WorkflowsDir\inv-reserved.json" @{
    cosmosDbEndpoint = @{ value = $cosmosEndpoint }
}
Deploy-LogicApp $invFailedName "$WorkflowsDir\inv-failed.json" @{
    cosmosDbEndpoint = @{ value = $cosmosEndpoint }
}
Deploy-LogicApp $shipSchedName "$WorkflowsDir\ship-sched.json" @{
    cosmosDbEndpoint = @{ value = $cosmosEndpoint }
}
# order-saga は callbackRegistrarUrl が空のまま初回デプロイ（Phase 3 で更新）
Deploy-LogicApp $orderSagaName "$WorkflowsDir\order-saga.json" @{
    kafkaPublisherUrl    = @{ value = $KafkaBridgeUrl }
    orderServiceUrl      = @{ value = $OrderServiceUrl }
    callbackRegistrarUrl = @{ value = '' }
}
Write-Host "  Logic Apps デプロイ完了" -ForegroundColor Green

# 1f. Cosmos DB SQL ロール付与 ────────────────────────────────────
Write-Host "  [1f] Cosmos DB SQL ロール付与中..."
$cosmosId  = "/subscriptions/$SubscriptionId/resourceGroups/$LogicAppRG/providers/Microsoft.DocumentDB/databaseAccounts/$cosmosName"
$roleDefId = "$cosmosId/sqlRoleDefinitions/00000000-0000-0000-0000-000000000002"

foreach ($laName in @($callbackRegName, $invReservedName, $invFailedName, $shipSchedName)) {
    $laPrincipalId = az logic workflow show -g $LogicAppRG -n $laName --query 'identity.principalId' -o tsv
    az cosmosdb sql role assignment create -g $LogicAppRG -a $cosmosName `
        --role-definition-id $roleDefId `
        --scope $cosmosId `
        --principal-id $laPrincipalId | Out-Null
    Write-Host "    Cosmos role assigned: $laName" -ForegroundColor DarkGray
}
Write-Host "  Cosmos DB ロール付与完了" -ForegroundColor Green
# ── フェーズ 1.5: Kafka ブリッジ Function App コードビルド & デプロイ ────────────────
Write-Host "`n=== Phase 1.5: Kafka ブリッジ Function App ビルド & デプロイ ===" -ForegroundColor Cyan

$kafkaBridgePomDir = Join-Path $PSScriptRoot "..\..\services\saga-orchestrator"

Write-Host "  Maven ビルド中 (skipTests)..."
push-location $kafkaBridgePomDir
mvn clean package -DskipTests `
    "-DfunctionAppName=$kafkaBridgeName"
if ($LASTEXITCODE -ne 0) {
    Pop-Location
    Write-Error "Maven build 失敗"
    exit 1
}

Write-Host "  Azure Functions デプロイ中..."
mvn azure-functions:deploy `
    "-DfunctionAppName=$kafkaBridgeName" `
    "-DfunctionAppResourceGroup=$LogicAppRG" `
    "-DfunctionAppRegion=$Location"
pop-location

if ($LASTEXITCODE -ne 0) {
    Write-Error "Azure Functions デプロイ失敗"
    exit 1
}
Write-Host "  Kafka ブリッジ Functions デプロイ完了: $kafkaBridgeName" -ForegroundColor Green
# ── フェーズ 2: CallbackRegistrar の Trigger URL を取得 ─────────
Write-Host "`n=== Phase 2: CallbackRegistrar Trigger URL 取得 ===" -ForegroundColor Cyan

$callbackRegUrl = az rest --method POST `
    --uri "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$LogicAppRG/providers/Microsoft.Logic/workflows/$callbackRegName/triggers/manual/listCallbackUrl?api-version=2019-05-01" `
    --query value -o tsv

if ([string]::IsNullOrEmpty($callbackRegUrl)) {
    Write-Error "CallbackRegistrar の Trigger URL を取得できませんでした"
    exit 1
}

Write-Host "  CALLBACK_REGISTRAR_URL: $callbackRegUrl" -ForegroundColor Green

# ── フェーズ 3: order-saga の callbackRegistrarUrl を更新 ───────
Write-Host "`n=== Phase 3: order-saga callbackRegistrarUrl 設定 ===" -ForegroundColor Cyan

Deploy-LogicApp $orderSagaName "$WorkflowsDir\order-saga.json" @{
    kafkaPublisherUrl    = @{ value = $KafkaBridgeUrl }
    orderServiceUrl      = @{ value = $OrderServiceUrl }
    callbackRegistrarUrl = @{ value = $callbackRegUrl }
}

Write-Host "  order-saga 更新完了" -ForegroundColor Green

# ── フェーズ 4: Consumer Logic App Trigger URL 取得 → Functions app settings 更新 ─
Write-Host "`n=== Phase 4: Consumer Logic App Trigger URL 取得 ===" -ForegroundColor Cyan

function Get-LogicAppTriggerUrl($rg, $name) {
    $url = az logic workflow trigger list-callback-url `
        --resource-group $rg `
        --workflow-name $name `
        --trigger-name "manual" `
        --query "value" -o tsv
    if ([string]::IsNullOrEmpty($url)) {
        throw "Trigger URL の取得に失敗しました: $name"
    }
    return $url
}

$invReservedUrl = Get-LogicAppTriggerUrl $LogicAppRG $invReservedName
$invFailedUrl   = Get-LogicAppTriggerUrl $LogicAppRG $invFailedName
$shipSchedUrl   = Get-LogicAppTriggerUrl $LogicAppRG $shipSchedName

Write-Host "  LOGIC_APP_INV_RESERVED_URL: $invReservedUrl" -ForegroundColor Green
Write-Host "  LOGIC_APP_INV_FAILED_URL  : $invFailedUrl" -ForegroundColor Green
Write-Host "  LOGIC_APP_SHIP_SCHED_URL  : $shipSchedUrl" -ForegroundColor Green

# Functions app settings を更新
$FuncAppName = $kafkaBridgeName
Write-Host "`n  Functions app ($FuncAppName) の app settings を更新中..."

$funcUri = "https://management.azure.com/subscriptions/$SubscriptionId" +
    "/resourceGroups/$LogicAppRG/providers/Microsoft.Web/sites/$FuncAppName" +
    "/config/appsettings?api-version=2022-03-01"
$props = (az rest --method GET --uri $funcUri | ConvertFrom-Json).properties
$props | Add-Member NoteProperty LOGIC_APP_INV_RESERVED_URL $invReservedUrl        -Force
$props | Add-Member NoteProperty LOGIC_APP_INV_FAILED_URL   $invFailedUrl          -Force
$props | Add-Member NoteProperty LOGIC_APP_SHIP_SCHED_URL   $shipSchedUrl          -Force
$props | Add-Member NoteProperty KAFKA_BOOTSTRAP_SERVERS    $KafkaBootstrapServers -Force
$tmpFile = [System.IO.Path]::GetTempFileName()
(@{ properties = $props } | ConvertTo-Json -Depth 5 -Compress) | Set-Content $tmpFile -Encoding utf8
az rest --method PUT --uri $funcUri --body "@$tmpFile" --headers 'Content-Type=application/json' `
    --query "properties.LOGIC_APP_INV_RESERVED_URL" -o tsv
Remove-Item $tmpFile -Force

if ($LASTEXITCODE -ne 0) {
    Write-Error "Functions app settings の更新に失敗しました"
    exit 1
}
Write-Host "  Functions app settings 更新完了" -ForegroundColor Green

# ── フェーズ 5: 動作確認 ───────────────────────────────────────
Write-Host "`n=== Phase 5: 動作確認 ===" -ForegroundColor Cyan

# 5つの Logic App が Enabled かチェック
$logicAppNames = @(
    "$($deployOutput.callbackRegName.value)"   # la-ketana-ext2-callback-reg
    "la-ketana-ext2-order-saga"
    "la-ketana-ext2-inv-reserved"
    "la-ketana-ext2-inv-failed"
    "la-ketana-ext2-ship-sched"
)

foreach ($name in $logicAppNames) {
    $state = az logic workflow show `
        --resource-group $LogicAppRG `
        --name $name `
        --query "state" -o tsv 2>$null
    if ($state -eq 'Enabled') {
        Write-Host "  ✓ $name : $state" -ForegroundColor Green
    } else {
        Write-Warning "  ✗ $name : $state (期待値: Enabled)"
    }
}

# OrderSagaOrchestrator の Trigger URL を取得して CALLBACK_REGISTRAR_URL を検証
Write-Host "`n  OrderSagaOrchestrator Trigger URL:"
$orderSagaUrl = az logic workflow trigger list-callback-url `
    --resource-group $LogicAppRG `
    --workflow-name $orderSagaName `
    --trigger-name "manual" `
    --query "value" -o tsv

Write-Host "  $orderSagaUrl" -ForegroundColor Yellow
Write-Host "  ↑ E2E テストでは ORDER_SAGA_ORCHESTRATOR_URL にこの URL を設定する"

# ── フェーズ 5: (オプション) Standard Logic App の削除 ──────────
Write-Host "`n=== Phase 5: Standard Logic App の削除 (確認後に実行) ===" -ForegroundColor Cyan
Write-Host "  旧 Standard Logic App はすでに削除済みです。" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  ※ Consumption Logic Apps の動作確認が完了してから削除すること" -ForegroundColor Yellow

Write-Host "`n=== デプロイ完了 ===" -ForegroundColor Green
Write-Host "ORDER_SAGA_ORCHESTRATOR_URL = $orderSagaUrl"
Write-Host "CALLBACK_REGISTRAR_URL      = $callbackRegUrl"

# spec/e2e-test.ps1 で使う環境変数を出力
Write-Host "`n--- E2E テスト用環境変数 (コピーして spec/0.config.md に反映すること) ---"
Write-Host "`$env:ORDER_SAGA_URL = `"$orderSagaUrl`""
Write-Host "`$env:CALLBACK_REGISTRAR_URL = `"$callbackRegUrl`""
