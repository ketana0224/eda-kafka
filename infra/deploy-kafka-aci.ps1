# deploy-kafka-aci.ps1
# ACI に apache/kafka (KRaft モード) をデプロイする
# 前提: az login 済み

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# --- 設定ファイル読み込み ---
$configPath = Join-Path $PSScriptRoot "config.psd1"
if (-not (Test-Path $configPath)) {
    throw "設定ファイルが見つかりません: $configPath"
}
$cfg = Import-PowerShellDataFile $configPath

$subscriptionId = $cfg.SubscriptionId
$resourceGroup  = $cfg.ResourceGroup
$location       = $cfg.Location
$containerGroup = $cfg.ContainerGroup
$kafkaImage     = $cfg.KafkaImage
$cpuCores       = $cfg.CpuCores
$memoryGb       = $cfg.MemoryGb

# -------------------------------------------------------
# Phase 1 — 前提条件確認
# -------------------------------------------------------
Write-Host "=== Phase 1: サブスクリプション設定 ===" -ForegroundColor Cyan

# サブスクリプションが存在するか確認
$subExists = az account list --query "[?id=='$subscriptionId'].id" -o tsv 2>$null
if (-not $subExists) {
    Write-Warning "サブスクリプション '$subscriptionId' が見つかりません。"
    Write-Host "テナント '$($cfg.TenantId)' で再ログインします..." -ForegroundColor Yellow
    az login --tenant $cfg.TenantId
    $subExists = az account list --query "[?id=='$subscriptionId'].id" -o tsv 2>$null
    if (-not $subExists) {
        throw "ログイン後もサブスクリプション '$subscriptionId' が見つかりません。アクセス権を確認してください。"
    }
}

az account set --subscription $subscriptionId
az account show --query "{id:id, name:name}" -o table

Write-Host "=== Phase 1: リソースプロバイダー確認・登録 ===" -ForegroundColor Cyan
foreach ($provider in @("Microsoft.ContainerInstance", "Microsoft.Network")) {
    $state = az provider show -n $provider --query registrationState -o tsv
    if ($state -ne "Registered") {
        Write-Host "  Registering $provider ..."
        az provider register -n $provider --wait
    } else {
        Write-Host "  ${provider}: Registered"
    }
}

Write-Host "=== Phase 1: リソースグループ確認・作成 ===" -ForegroundColor Cyan
$rgExists = az group show --name $resourceGroup --query name -o tsv 2>$null
if ($rgExists) {
    Write-Warning "リソースグループ '$resourceGroup' は既に存在します。"
    $confirm = Read-Host "削除して再作成しますか？ (yes/no)"
    if ($confirm -eq "yes") {
        Write-Host "  削除中..." -ForegroundColor Yellow
        az group delete --name $resourceGroup --yes
        do {
            Start-Sleep -Seconds 10
            $state = az group show --name $resourceGroup --query properties.provisioningState -o tsv 2>$null
            if ($state) { Write-Host "  削除待機中: $state" }
        } while ($state)
        Write-Host "  削除完了" -ForegroundColor Green
        az group create --name $resourceGroup --location $location --output table
    } else {
        Write-Host "  既存のリソースグループを使用します" -ForegroundColor DarkGray
    }
} else {
    az group create --name $resourceGroup --location $location --output table
}

# -------------------------------------------------------
# Phase 2 — ACI に Kafka コンテナをデプロイ（パブリックIP）
# -------------------------------------------------------
Write-Host "=== Phase 2: ACI コンテナグループ作成（1回目 — IP取得用） ===" -ForegroundColor Cyan
$createArgs1 = @(
    "container", "create",
    "--name",           $containerGroup,
    "--resource-group", $resourceGroup,
    "--location",       $location,
    "--image",          $kafkaImage,
    "--os-type",        "Linux",
    "--cpu",            $cpuCores,
    "--memory",         $memoryGb,
    "--ports",          "9092", "9093",
    "--ip-address",     "Public",
    "--environment-variables",
        "CLUSTER_ID=5L6g3nShT-eMCtK--X86sw",
        "KAFKA_NODE_ID=1",
        "KAFKA_PROCESS_ROLES=broker,controller",
        "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
        "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
        "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
    "--output", "table"
)
az @createArgs1
if ($LASTEXITCODE -ne 0) { throw "ACI コンテナグループの作成に失敗しました" }

$aciIp = az container show `
    --name $containerGroup `
    --resource-group $resourceGroup `
    --query ipAddress.ip -o tsv
Write-Host "ACI IP: $aciIp" -ForegroundColor Green
if (-not $aciIp) { throw "ACI の IP アドレスが取得できませんでした" }

Write-Host "=== Phase 2: ADVERTISED_LISTENERS を確定IP で更新 ===" -ForegroundColor Cyan
$createArgs2 = @(
    "container", "create",
    "--name",           $containerGroup,
    "--resource-group", $resourceGroup,
    "--location",       $location,
    "--image",          $kafkaImage,
    "--os-type",        "Linux",
    "--cpu",            $cpuCores,
    "--memory",         $memoryGb,
    "--ports",          "9092", "9093",
    "--ip-address",     "Public",
    "--environment-variables",
        "CLUSTER_ID=5L6g3nShT-eMCtK--X86sw",
        "KAFKA_NODE_ID=1",
        "KAFKA_PROCESS_ROLES=broker,controller",
        "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${aciIp}:9092",
        "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
        "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
    "--output", "table"
)
az @createArgs2
if ($LASTEXITCODE -ne 0) { throw "ACI コンテナグループの更新に失敗しました" }

# -------------------------------------------------------
# Phase 4 — EntraID ロール割り当て
# -------------------------------------------------------
Write-Host "=== Phase 4: Contributor ロール割り当て ===" -ForegroundColor Cyan
$currentUser = az ad signed-in-user show --query id -o tsv
$rgScope = "/subscriptions/$subscriptionId/resourceGroups/$resourceGroup"

$existing = az role assignment list --assignee $currentUser --role Contributor --scope $rgScope --query "[].id" -o tsv
if (-not $existing) {
    az role assignment create `
        --assignee $currentUser `
        --role Contributor `
        --scope $rgScope `
        --output table
    Write-Host "  Contributor ロールを付与しました"
} else {
    Write-Host "  Contributor ロールは既に付与済み"
}

# -------------------------------------------------------
# 完了サマリー
# -------------------------------------------------------
Write-Host ""
Write-Host "=== デプロイ完了 ===" -ForegroundColor Green
Write-Host "Kafka Bootstrap: ${aciIp}:9092"
az container show --name $containerGroup --resource-group $resourceGroup `
    --query "{state:instanceView.state, ip:ipAddress.ip}" -o table
