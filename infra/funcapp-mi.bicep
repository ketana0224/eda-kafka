// Function App with System-Assigned Managed Identity for storage access
// allowSharedKeyAccess:false に対応 — ARM(AAD) でファイルシェア作成、データプレーンは RBAC
@description('Function App 名')
param functionAppName string = 'func-ketana-ext2-eda-saga'

@description('既存 Storage Account 名（allowSharedKeyAccess: false）')
param storageAccountName string = 'stketanaext2saga'

param location string = 'japaneast'
param appInsightsInstrumentationKey string

@description('コンテンツ用ファイルシェア名（小文字 63 文字以内）')
param contentShareName string = 'func-saga-content'

// ─── 既存リソース参照 ───────────────────────────────────────────
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}

resource fileService 'Microsoft.Storage/storageAccounts/fileServices@2023-01-01' existing = {
  parent: storageAccount
  name: 'default'
}

// ─── ファイルシェアを ARM (AAD) で作成 ───────────────────────────
// ARM は AAD 認証で動作するため allowSharedKeyAccess: false でも作成可能
resource fileShare 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-01-01' = {
  parent: fileService
  name: contentShareName
  properties: {
    shareQuota: 5120
    enabledProtocols: 'SMB'
  }
}

// ─── 既存 Container Apps 環境参照 ──────────────────────────────
resource acaEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' existing = {
  name: 'aca-env-ketana-eda-kafka'
}

// ─── Function App on Container Apps (System-Assigned MI) ──────
resource functionApp 'Microsoft.Web/sites@2023-12-01' = {
  name: functionAppName
  location: location
  kind: 'functionapp'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    managedEnvironmentId: acaEnvironment.id
    siteConfig: {
      // AzureWebJobsStorage → MI 接続（__ サフィックス形式）
      appSettings: [
        {
          name: 'AzureWebJobsStorage__accountName'
          value: storageAccountName
        }
        {
          name: 'AzureWebJobsStorage__credential'
          value: 'managedidentity'
        }
        // コンテンツ共有 → MI 接続
        {
          name: 'WEBSITE_CONTENTAZUREFILECONNECTIONSTRING__accountName'
          value: storageAccountName
        }
        {
          name: 'WEBSITE_CONTENTAZUREFILECONNECTIONSTRING__credential'
          value: 'managedidentity'
        }
        {
          name: 'WEBSITE_CONTENTSHARE'
          value: contentShareName
        }
        {
          name: 'FUNCTIONS_EXTENSION_VERSION'
          value: '~4'
        }
        {
          name: 'FUNCTIONS_WORKER_RUNTIME'
          value: 'java'
        }
        {
          name: 'APPINSIGHTS_INSTRUMENTATIONKEY'
          value: appInsightsInstrumentationKey
        }
      ]
    }
  }
  dependsOn: [fileShare]
}

// ─── RBAC ロール割り当て ────────────────────────────────────────

// Storage Blob Data Owner: AzureWebJobsStorage (blobs/containers)
resource blobOwnerRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, functionApp.id, 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b')
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// Storage Queue Data Contributor: AzureWebJobsStorage (queues)
resource queueRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, functionApp.id, '974c5e8b-45b9-4653-ba55-5f855dd0fb88')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '974c5e8b-45b9-4653-ba55-5f855dd0fb88')
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// Storage Table Data Contributor: AzureWebJobsStorage (tables)
resource tableRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, functionApp.id, '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3')
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// Storage File Data SMB Share Contributor: コンテンツ用ファイルシェア
resource fileShareRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, functionApp.id, '0c867c2a-1d8c-454a-a3db-ab2ea1bdc8bb')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '0c867c2a-1d8c-454a-a3db-ab2ea1bdc8bb')
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

output functionAppName string = functionApp.name
output principalId string = functionApp.identity.principalId
