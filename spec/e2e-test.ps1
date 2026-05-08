# ============================================================
# E2E シナリオ確認スクリプト
#
# 前提条件:
#   - Phase 1〜7 が完了していること
#   - カレントディレクトリは C:\GitHub\eda-kafka
#
# 実行方法（シナリオ単位）:
#   . .\spec\e2e-test.ps1   # 変数・関数を読み込む
#   Test-Scenario10          # 正常フロー
#   Test-Scenario11          # 補償フロー（在庫不足）
#   Test-Scenario12          # タイムアウトフロー
# ============================================================

Set-Location C:\GitHub\eda-kafka
$cfg     = Import-PowerShellDataFile .\infra\config.psd1
$funcUrl = "https://func-ketana-ext2-saga-orch.azurewebsites.net/api/orders"

# PostgreSQL 接続パスワード（Phase 3 で設定したもの）
$pgPasswordSecure = Read-Host "PostgreSQL admin password" -AsSecureString
$pgPassword = [System.Net.NetworkCredential]::new("", $pgPasswordSecure).Password

# psql がない場合のみ rdbms-connect 拡張をインストール
if (Get-Command psql -ErrorAction SilentlyContinue) {
    Write-Host "psql 使用: $(Get-Command psql | Select-Object -ExpandProperty Source)" -ForegroundColor DarkGray
} else {
    $extName = az extension show --name rdbms-connect --query name -o tsv 2>$null
    if (-not $extName) {
        Write-Host "rdbms-connect 拡張をインストール中..." -ForegroundColor Yellow
        az extension add --name rdbms-connect --upgrade --yes
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "rdbms-connect 拡張のインストールに失敗しました。psql をインストールするか、テストデータを手動で投入してください。"
        }
    }
}

# ==============================================================
# PostgreSQL サーバー起動（自動停止から復旧）
# ==============================================================
$pgServers = @("psql-ketana-ext2-order", "psql-ketana-ext2-inventory", "psql-ketana-ext2-shipping")

Write-Host "`n=== PostgreSQL サーバー状態確認 ===" -ForegroundColor Cyan
foreach ($srv in $pgServers) {
    $state = az postgres flexible-server show `
        -g $cfg.ResourceGroup -n $srv `
        --query state -o tsv 2>$null
    if ($state -eq "Stopped") {
        Write-Host "  $srv : Stopped → 起動中..." -ForegroundColor Yellow
        az postgres flexible-server start -g $cfg.ResourceGroup -n $srv --no-wait | Out-Null
    } else {
        Write-Host "  $srv : $state" -ForegroundColor DarkGray
    }
}

# 起動を待つ（Stopped だったサーバーが Running になるまで最大 3 分）
$needWait = $false
foreach ($srv in $pgServers) {
    $state = az postgres flexible-server show `
        -g $cfg.ResourceGroup -n $srv `
        --query state -o tsv 2>$null
    if ($state -ne "Ready") { $needWait = $true }
}
if ($needWait) {
    Write-Host "  起動待ち（最大 3 分）..." -ForegroundColor Yellow
    $deadline = (Get-Date).AddMinutes(3)
    do {
        Start-Sleep -Seconds 15
        $allReady = $true
        foreach ($srv in $pgServers) {
            $state = az postgres flexible-server show `
                -g $cfg.ResourceGroup -n $srv `
                --query state -o tsv 2>$null
            if ($state -ne "Ready") { $allReady = $false }
        }
    } while (-not $allReady -and (Get-Date) -lt $deadline)

    if ($allReady) {
        Write-Host "  全 PostgreSQL サーバー Ready ✅" -ForegroundColor Green
    } else {
        Write-Warning "  タイムアウト: 一部サーバーがまだ起動中です。少し待ってから再試行してください。"
    }
} else {
    Write-Host "  全 PostgreSQL サーバー Ready ✅" -ForegroundColor Green
}

# ダッシュボード URL（確認用）
$dashboardUrl = az durabletask taskhub show `
  -g $cfg.ResourceGroup `
  --scheduler-name "dts-ketana-ext2-saga" `
  --name "sagahub" `
  --query properties.dashboardUrl -o tsv
Write-Host "Dashboard: $dashboardUrl" -ForegroundColor DarkCyan

# ==============================================================
# ヘルパー関数
# ==============================================================

# オーケストレーション完了待ち（statusQueryGetUri をポーリング）
function Wait-Orchestration {
    param(
        [string]$StatusUri,
        [int]$TimeoutSec = 300,
        [int]$IntervalSec = 5
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    do {
        Start-Sleep -Seconds $IntervalSec
        try {
            $resp = Invoke-RestMethod -Uri $StatusUri -Method GET -ErrorAction Stop
        } catch {
            Write-Warning "  ポーリング失敗: $_"
            continue
        }
        Write-Host "  $(Get-Date -Format 'HH:mm:ss')  runtimeStatus: $($resp.runtimeStatus)"
        if ($resp.runtimeStatus -in @("Completed","Failed","Terminated")) {
            return $resp
        }
    } while ((Get-Date) -lt $deadline)
    Write-Warning "タイムアウト: ${TimeoutSec}s 以内に完了しなかった"
    return $null
}

# PostgreSQL クエリ実行（psql 優先、なければ az postgres flexible-server connect を使用）
function Invoke-PgQuery {
    param(
        [string]$ServerName,
        [string]$Database,
        [string]$Query
    )
    $pgHost = "${ServerName}.postgres.database.azure.com"
    if (Get-Command psql -ErrorAction SilentlyContinue) {
        $env:PGPASSWORD = $pgPassword
        psql "host=$pgHost port=5432 dbname=$Database user=sagaadmin sslmode=require" -c $Query 2>&1
        Remove-Item env:PGPASSWORD -ErrorAction SilentlyContinue
    } else {
        # --output は connect コマンドでは無効なため指定しない
        az postgres flexible-server connect `
            --name $ServerName `
            --admin-user sagaadmin `
            --admin-password $pgPassword `
            --database-name $Database `
            --querytext $Query 2>&1
    }
}

# Saga HTTP Trigger 呼び出し（202 Accepted + statusQueryGetUri を返す）
function Start-OrderSaga {
    param(
        [string]$CustomerId,
        [string]$ProductId,
        [int]$Quantity,
        [long]$TotalAmount
    )
    $body = @{
        customerId      = $CustomerId
        shippingAddress = "Tokyo, Japan"
        items           = @(@{ productId = $ProductId; quantity = $Quantity })
        totalAmount     = $TotalAmount
    } | ConvertTo-Json

    try {
        $resp = Invoke-WebRequest -Method POST -Uri $funcUrl `
            -ContentType "application/json" `
            -Body $body -UseBasicParsing -TimeoutSec 60
        Write-Host "  StatusCode  : $($resp.StatusCode)"
        $json = $resp.Content | ConvertFrom-Json
        Write-Host "  instanceId  : $($json.id)"
        Write-Host "  statusUri   : $($json.statusQueryGetUri)"
        return $json
    } catch {
        Write-Host "  StatusCode: $($_.Exception.Response.StatusCode.value__)" -ForegroundColor Red
        return $null
    }
}

# ==============================================================
# テストデータ投入
# ==============================================================
# ※ JPA の ddl-auto: update でテーブルは自動生成される
# ※ 初回実行時のみ必要（ON CONFLICT で冪等）
function Initialize-TestData {
    Write-Host "`n=== テストデータ投入 ===" -ForegroundColor Cyan

    # テーブル作成（存在しない場合）
    Invoke-PgQuery `
        -ServerName "psql-ketana-ext2-inventory" -Database "inventorydb" `
        -Query "CREATE TABLE IF NOT EXISTS products (product_id VARCHAR(255) PRIMARY KEY, name VARCHAR(255) NOT NULL, stock_quantity INT NOT NULL DEFAULT 0);" | Out-Null
    Write-Host "  テーブル確認/作成完了"

    # 在庫あり商品（シナリオ10用）
    Invoke-PgQuery `
        -ServerName "psql-ketana-ext2-inventory" -Database "inventorydb" `
        -Query "INSERT INTO products (product_id, name, stock_quantity) VALUES ('prod-normal', 'Normal Product', 100) ON CONFLICT (product_id) DO UPDATE SET stock_quantity = 100;"
    Write-Host "  prod-normal : stock=100"

    # 在庫なし商品（シナリオ11用）
    Invoke-PgQuery `
        -ServerName "psql-ketana-ext2-inventory" -Database "inventorydb" `
        -Query "INSERT INTO products (product_id, name, stock_quantity) VALUES ('prod-empty', 'Empty Product', 0) ON CONFLICT (product_id) DO UPDATE SET stock_quantity = 0;"
    Write-Host "  prod-empty  : stock=0"

    Write-Host "テストデータ投入完了`n" -ForegroundColor Green
}

# ==============================================================
# シナリオ 10 — 正常フロー
#
# 期待値:
#   - runtimeStatus = Completed
#   - orders テーブルの status = CONFIRMED
# ==============================================================
function Test-Scenario10 {
    Write-Host "`n==============================" -ForegroundColor Green
    Write-Host " シナリオ 10: 正常フロー" -ForegroundColor Green
    Write-Host "==============================`n" -ForegroundColor Green

    # prod-normal の在庫を 100 にリセット
    Invoke-PgQuery `
        -ServerName "psql-ketana-ext2-inventory" -Database "inventorydb" `
        -Query "UPDATE products SET stock_quantity = 100 WHERE product_id = 'prod-normal';" | Out-Null

    Write-Host "① Saga 起動（prod-normal, qty=1）..."
    $saga = Start-OrderSaga -CustomerId "customer-e2e-10" `
        -ProductId "prod-normal" -Quantity 1 -TotalAmount 1000
    if (-not $saga) { Write-Host "Saga 起動失敗" -ForegroundColor Red; return }

    Write-Host "`n② オーケストレーション完了待ち（最大 5 分）..."
    $result = Wait-Orchestration -StatusUri $saga.statusQueryGetUri -TimeoutSec 300

    if ($result -and $result.runtimeStatus -eq "Completed") {
        Write-Host "`n③ Orchestration 完了 ✅" -ForegroundColor Green

        # orders テーブルで status 確認（orderId は output から取得できる場合）
        Write-Host "`n④ orders テーブル確認（最新 1 件）..."
        Invoke-PgQuery `
            -ServerName "psql-ketana-ext2-order" -Database "orderdb" `
            -Query "SELECT order_id, customer_id, status, created_at FROM orders ORDER BY created_at DESC LIMIT 1;"

        Write-Host "`n期待値: status = CONFIRMED" -ForegroundColor Yellow
    } else {
        Write-Host "`n❌ 期待する状態にならなかった: $($result.runtimeStatus)" -ForegroundColor Red
        Write-Host "output: $($result.output)"
    }
}

# ==============================================================
# シナリオ 11 — 補償フロー（在庫不足）
#
# 期待値:
#   - runtimeStatus = Completed（補償完了も Completed）
#   - orders テーブルの status = CANCELLED
# ==============================================================
function Test-Scenario11 {
    Write-Host "`n==============================" -ForegroundColor Yellow
    Write-Host " シナリオ 11: 補償フロー（在庫不足）" -ForegroundColor Yellow
    Write-Host "==============================`n" -ForegroundColor Yellow

    Write-Host "① Saga 起動（prod-empty, qty=1 → stock=0 のため在庫不足）..."
    $saga = Start-OrderSaga -CustomerId "customer-e2e-11" `
        -ProductId "prod-empty" -Quantity 1 -TotalAmount 500
    if (-not $saga) { Write-Host "Saga 起動失敗" -ForegroundColor Red; return }

    Write-Host "`n② オーケストレーション完了待ち（最大 5 分）..."
    $result = Wait-Orchestration -StatusUri $saga.statusQueryGetUri -TimeoutSec 300

    if ($result -and $result.runtimeStatus -eq "Completed") {
        Write-Host "`n③ Orchestration 完了（補償済み）✅" -ForegroundColor Green

        Write-Host "`n④ orders テーブル確認（最新 1 件）..."
        Invoke-PgQuery `
            -ServerName "psql-ketana-ext2-order" -Database "orderdb" `
            -Query "SELECT order_id, customer_id, status, created_at FROM orders ORDER BY created_at DESC LIMIT 1;"

        Write-Host "`n期待値: status = CANCELLED" -ForegroundColor Yellow
    } else {
        Write-Host "`n❌ 期待する状態にならなかった: $($result.runtimeStatus)" -ForegroundColor Red
        Write-Host "output: $($result.output)"
    }
}

# ==============================================================
# シナリオ 12 — タイムアウトフロー（InventoryService 停止）
#
# 注意: SAGA_TIMEOUT_MINUTES=30 → 完了まで最大 30 分かかる
#       このスクリプトはタイムアウト発火の確認は行わず、
#       Saga 起動 → InventoryService 停止 → ステータス確認 URI を出力する。
#       30 分後に Check-Scenario12 を実行して結果を確認する。
#
# 期待値:
#   - 30 分後に runtimeStatus = Completed（タイムアウト補償完了）
#   - orders テーブルの status = CANCELLED
# ==============================================================
function Test-Scenario12 {
    Write-Host "`n==============================" -ForegroundColor Magenta
    Write-Host " シナリオ 12: タイムアウトフロー" -ForegroundColor Magenta
    Write-Host "==============================`n" -ForegroundColor Magenta

    # prod-normal の在庫を補充
    Invoke-PgQuery `
        -ServerName "psql-ketana-ext2-inventory" -Database "inventorydb" `
        -Query "UPDATE products SET stock_quantity = 100 WHERE product_id = 'prod-normal';" | Out-Null

    Write-Host "① InventoryService (aca-inventory) をスケール 0 に設定..."
    az containerapp update `
        --name "aca-inventory" `
        --resource-group $cfg.ResourceGroup `
        --min-replicas 0 `
        --max-replicas 0 `
        --output none
    Write-Host "  aca-inventory: min/max replicas = 0（停止中）" -ForegroundColor Yellow

    Start-Sleep -Seconds 10  # レプリカ停止を待つ

    Write-Host "`n② Saga 起動（prod-normal, qty=1）..."
    $saga = Start-OrderSaga -CustomerId "customer-e2e-12" `
        -ProductId "prod-normal" -Quantity 1 -TotalAmount 1000
    if (-not $saga) {
        # InventoryService を元に戻してから終了
        az containerapp update --name "aca-inventory" --resource-group $cfg.ResourceGroup `
            --min-replicas 1 --max-replicas 10 --output none
        Write-Host "Saga 起動失敗" -ForegroundColor Red
        return
    }

    # ステータス URI を保存（後で確認するため）
    $statusUri = $saga.statusQueryGetUri
    $statusUri | Set-Content -Path ".\spec\.scenario12-status-uri.txt"
    Write-Host "`n③ statusQueryGetUri を .\spec\.scenario12-status-uri.txt に保存"
    Write-Host "   SAGA_TIMEOUT_MINUTES=30 のため、30 分後に Check-Scenario12 を実行すること"
    Write-Host "   statusQueryGetUri: $statusUri" -ForegroundColor DarkCyan

    Write-Host "`n④ 現在のオーケストレーション状態（Pending/Running のはず）..."
    Start-Sleep -Seconds 5
    try {
        $resp = Invoke-RestMethod -Uri $statusUri -Method GET
        Write-Host "  runtimeStatus: $($resp.runtimeStatus)"
    } catch {
        Write-Warning "  取得失敗: $_"
    }

    Write-Host "`n[注意] InventoryService は停止中のまま。30 分後に Check-Scenario12 を実行。" -ForegroundColor Yellow
}

# シナリオ 12 の結果確認（Saga 起動から 30 分後に実行）
function Check-Scenario12 {
    Write-Host "`n=== シナリオ 12 結果確認 ===" -ForegroundColor Magenta

    $uriFile = ".\spec\.scenario12-status-uri.txt"
    if (-not (Test-Path $uriFile)) {
        Write-Warning "statusUri ファイルが見つからない: $uriFile"
        return
    }
    $statusUri = Get-Content $uriFile

    Write-Host "① オーケストレーション状態確認..."
    try {
        $resp = Invoke-RestMethod -Uri $statusUri -Method GET
        Write-Host "  runtimeStatus: $($resp.runtimeStatus)"
        Write-Host "期待値: Completed（タイムアウト補償完了）" -ForegroundColor Yellow
    } catch {
        Write-Warning "  取得失敗: $_"
    }

    Write-Host "`n② orders テーブル確認..."
    Invoke-PgQuery `
        -ServerName "psql-ketana-ext2-order" -Database "orderdb" `
        -Query "SELECT order_id, customer_id, status, created_at FROM orders ORDER BY created_at DESC LIMIT 1;"
    Write-Host "期待値: status = CANCELLED" -ForegroundColor Yellow

    Write-Host "`n③ InventoryService を元に戻す..."
    az containerapp update `
        --name "aca-inventory" `
        --resource-group $cfg.ResourceGroup `
        --min-replicas 1 `
        --max-replicas 10 `
        --output none
    Write-Host "  aca-inventory: min-replicas = 1（復元完了）" -ForegroundColor Green

    Remove-Item $uriFile -ErrorAction SilentlyContinue
}

# ==============================================================
# 全シナリオ一括実行
# ==============================================================
function Test-AllScenarios {
    Initialize-TestData
    Test-Scenario10
    Test-Scenario11
    # シナリオ 12 は 30 分待機が必要なため別途実行する
    Write-Host "`n[シナリオ 12 は Test-Scenario12 を個別に実行してください]" -ForegroundColor Magenta
}

Write-Host "`n関数読み込み完了。以下を実行してください:" -ForegroundColor Cyan
Write-Host "  Initialize-TestData   # テストデータ投入（初回のみ）"
Write-Host "  Test-Scenario10       # 正常フロー"
Write-Host "  Test-Scenario11       # 補償フロー（在庫不足）"
Write-Host "  Test-Scenario12       # タイムアウト（30 分後に Check-Scenario12）"
Write-Host "  Test-AllScenarios     # 10・11 を連続実行"
Write-Host "  Dashboard: $dashboardUrl" -ForegroundColor DarkCyan
