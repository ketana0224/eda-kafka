// ============================================================
// Logic Apps Consumption — Saga Orchestrator (Option A)
// Kafka Managed Connector は Consumption プランで利用不可。
// Kafka ブリッジに Azure Functions を使用:
//   - KafkaPublisherFunction: HTTP → Kafka (Logic App が呼び出す)
//   - KafkaConsumerFunctions: Kafka → Logic App HTTP trigger
// ============================================================

@description('Azure region. デフォルト: リソースグループのロケーション')
param location string = resourceGroup().location

@description('Kafka broker 接続先。例: 1.2.3.4:9092')
param kafkaBootstrapServers string

@description('Order Service ベース URL。例: https://order-svc.example.com')
param orderServiceUrl string

@description('CallbackRegistrar の trigger URL。Phase 2 で設定する。初回デプロイは空のまま。')
param callbackRegistrarUrl string = ''

@description('Cosmos DB アカウント名')
param cosmosDbAccountName string = 'cosmos-ketana-ext2-saga'

// ── 変数 ─────────────────────────────────────────────────────
var prefix = 'la-ketana-ext2'
// Cosmos DB Built-in Data Contributor (固定 GUID)
var cosmosDataContributorRoleId = '00000000-0000-0000-0000-000000000002'

// ── Cosmos DB ─────────────────────────────────────────────────
resource cosmosDb 'Microsoft.DocumentDB/databaseAccounts@2023-04-15' = {
  name: cosmosDbAccountName
  location: location
  kind: 'GlobalDocumentDB'
  properties: {
    databaseAccountOfferType: 'Standard'
    locations: [{ locationName: location, failoverPriority: 0, isZoneRedundant: false }]
    consistencyPolicy: { defaultConsistencyLevel: 'Session' }
    disableLocalAuth: false // 本番では true に変更してマスターキーを無効化
    publicNetworkAccess: 'Enabled'
  }
}

resource sagaDb 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2023-04-15' = {
  parent: cosmosDb
  name: 'sagadb'
  properties: {
    resource: { id: 'sagadb' }
    options: { throughput: 400 }
  }
}

resource callbackUrlsColl 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2023-04-15' = {
  parent: sagaDb
  name: 'CallbackUrls'
  properties: {
    resource: {
      id: 'CallbackUrls'
      partitionKey: { paths: ['/orchestrationId'], kind: 'Hash' }
      defaultTtl: 7200 // 2 時間で自動削除
    }
  }
}

// ── Cosmos DB ロール付与ヘルパー ─────────────────────────────
// 各 Logic App MSI に Cosmos DB Built-in Data Contributor を付与する。
// scope は アカウントレベル (コンテナまで継承される)。

// ── 1. CallbackRegistrar ──────────────────────────────────────
resource callbackReg 'Microsoft.Logic/workflows@2019-05-01' = {
  name: '${prefix}-callback-reg'
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {
    state: 'Enabled'
    definition: {
      '$schema': 'https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#'
      contentVersion: '1.0.0.0'
      parameters: {
        cosmosDbEndpoint: { type: 'String', defaultValue: '' }
      }
      triggers: {
        manual: {
          type: 'Request'
          kind: 'Http'
          inputs: {
            schema: {
              type: 'object'
              properties: {
                orchestrationId: { type: 'string' }
                callbackUrl: { type: 'string' }
              }
              required: ['orchestrationId', 'callbackUrl']
            }
          }
        }
      }
      actions: {
        Upsert_CallbackUrl: {
          type: 'Http'
          runAfter: {}
          inputs: {
            method: 'PUT'
            // Cosmos DB REST API: PUT でドキュメントを upsert
            uri: '@{concat(parameters(''cosmosDbEndpoint''), ''dbs/sagadb/colls/CallbackUrls/docs/'', triggerBody()[''orchestrationId''])}'
            headers: {
              'Content-Type': 'application/json'
              // パーティションキーは JSON 配列文字列で指定
              'x-ms-documentdb-partitionkey': '@{concat(''["'', triggerBody()[''orchestrationId''], ''"]'')}'
              'x-ms-documentdb-is-upsert': 'true'
            }
            body: {
              id: '@{triggerBody()[''orchestrationId'']}'
              orchestrationId: '@{triggerBody()[''orchestrationId'']}'
              callbackUrl: '@{triggerBody()[''callbackUrl'']}'
            }
            authentication: {
              type: 'ManagedServiceIdentity'
              audience: 'https://cosmos.azure.com'
            }
          }
        }
        Response_OK: {
          type: 'Response'
          runAfter: { Upsert_CallbackUrl: ['Succeeded'] }
          inputs: { statusCode: 200 }
        }
      }
      outputs: {}
    }
    parameters: {
      cosmosDbEndpoint: { value: cosmosDb.properties.documentEndpoint }
    }
  }
}

resource callbackRegCosmosRole 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2023-04-15' = {
  parent: cosmosDb
  name: guid(cosmosDb.id, callbackReg.id, cosmosDataContributorRoleId)
  properties: {
    roleDefinitionId: '${cosmosDb.id}/sqlRoleDefinitions/${cosmosDataContributorRoleId}'
    principalId: callbackReg.identity.principalId
    scope: cosmosDb.id
  }
  dependsOn: [callbackUrlsColl]
}

// ── 2. OrderSagaOrchestrator ──────────────────────────────────
resource orderSaga 'Microsoft.Logic/workflows@2019-05-01' = {
  name: '${prefix}-order-saga'
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {
    state: 'Enabled'
    definition: {
      '$schema': 'https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#'
      contentVersion: '1.0.0.0'
      parameters: {
        kafkaPublisherUrl: { type: 'String', defaultValue: '' }
        orderServiceUrl: { type: 'String', defaultValue: '' }
        callbackRegistrarUrl: { type: 'String', defaultValue: '' }
      }
      triggers: {
        manual: {
          type: 'Request'
          kind: 'Http'
          inputs: {
            schema: {
              type: 'object'
              properties: {
                customerId: { type: 'string' }
                shippingAddress: { type: 'string' }
                items: { type: 'array' }
                totalAmount: { type: 'number' }
              }
              required: ['customerId', 'items']
            }
          }
        }
      }
      actions: {
        Create_Order: {
          type: 'Http'
          runAfter: {}
          inputs: {
            method: 'POST'
            uri: '@{concat(parameters(''orderServiceUrl''), ''/api/orders'')}'
            headers: { 'Content-Type': 'application/json' }
            body: '@triggerBody()'
          }
        }
        Parse_Order_Response: {
          type: 'ParseJson'
          runAfter: { Create_Order: ['Succeeded'] }
          inputs: {
            content: '@body(''Create_Order'')'
            schema: {
              type: 'object'
              properties: { orderId: { type: 'string' } }
            }
          }
        }
        Send_Response: {
          type: 'Response'
          runAfter: { Parse_Order_Response: ['Succeeded'] }
          inputs: {
            statusCode: 202
            body: {
              runId: '@{workflow().run.name}'
              orderId: '@{body(''Parse_Order_Response'')[''orderId'']}'
              status: 'accepted'
            }
          }
        }
        Publish_InventoryReserveCommand: {
          type: 'Http'
          runAfter: { Send_Response: ['Succeeded'] }
          inputs: {
            method: 'POST'
            uri: '@parameters(''kafkaPublisherUrl'')'
            headers: { 'Content-Type': 'application/json' }
            body: {
              topic: 'inventory.reserve.command'
              key: '@{body(''Parse_Order_Response'')[''orderId'']}'
              value: '@{string(createObject(''orderId'', body(''Parse_Order_Response'')[''orderId''], ''items'', triggerBody()[''items''], ''orchestrationId'', concat(workflow().run.name, ''-inv''), ''issuedAt'', utcNow()))}'
            }
          }
        }
        Wait_For_InventoryResponse: {
          type: 'HttpWebhook'
          runAfter: { Publish_InventoryReserveCommand: ['Succeeded'] }
          inputs: {
            subscribe: {
              method: 'POST'
              uri: '@parameters(''callbackRegistrarUrl'')'
              headers: { 'Content-Type': 'application/json' }
              body: {
                orchestrationId: '@{concat(workflow().run.name, ''-inv'')}'
                callbackUrl: '@{listCallbackUrl()}'
              }
            }
            unsubscribe: {}
          }
          limit: { timeout: 'PT30M' }
        }
        Check_Inventory_Success: {
          type: 'If'
          runAfter: {
            Wait_For_InventoryResponse: ['Succeeded', 'Failed', 'TimedOut', 'Skipped']
          }
          expression: {
            and: [{ equals: ['@actions(''Wait_For_InventoryResponse'')[''status'']', 'Succeeded'] }]
          }
          actions: {
            Publish_ShippingScheduleCommand: {
              type: 'Http'
              runAfter: {}
              inputs: {
                method: 'POST'
                uri: '@parameters(''kafkaPublisherUrl'')'
                headers: { 'Content-Type': 'application/json' }
                body: {
                  topic: 'shipping.schedule.command'
                  key: '@{body(''Parse_Order_Response'')[''orderId'']}'
                  value: '@{string(createObject(''orderId'', body(''Parse_Order_Response'')[''orderId''], ''shippingAddress'', triggerBody()[''shippingAddress''], ''orchestrationId'', concat(workflow().run.name, ''-ship''), ''issuedAt'', utcNow()))}'
                }
              }
            }
            Wait_For_ShippingResponse: {
              type: 'HttpWebhook'
              runAfter: { Publish_ShippingScheduleCommand: ['Succeeded'] }
              inputs: {
                subscribe: {
                  method: 'POST'
                  uri: '@parameters(''callbackRegistrarUrl'')'
                  headers: { 'Content-Type': 'application/json' }
                  body: {
                    orchestrationId: '@{concat(workflow().run.name, ''-ship'')}'
                    callbackUrl: '@{listCallbackUrl()}'
                  }
                }
                unsubscribe: {}
              }
              limit: { timeout: 'PT30M' }
            }
            Check_Shipping_Success: {
              type: 'If'
              runAfter: {
                Wait_For_ShippingResponse: ['Succeeded', 'Failed', 'TimedOut', 'Skipped']
              }
              expression: {
                and: [{ equals: ['@actions(''Wait_For_ShippingResponse'')[''status'']', 'Succeeded'] }]
              }
              actions: {
                Publish_OrderConfirmed: {
                  type: 'Http'
                  runAfter: {}
                  inputs: {
                    method: 'POST'
                    uri: '@parameters(''kafkaPublisherUrl'')'
                    headers: { 'Content-Type': 'application/json' }
                    body: {
                      topic: 'order.confirmed'
                      key: '@{body(''Parse_Order_Response'')[''orderId'']}'
                      value: '@{string(createObject(''orderId'', body(''Parse_Order_Response'')[''orderId''], ''orchestrationId'', workflow().run.name, ''occurredAt'', utcNow()))}'
                    }
                  }
                }
              }
              else: {
                actions: {
                  Publish_InventoryReleaseCommand_OnShipFail: {
                    type: 'Http'
                    runAfter: {}
                    inputs: {
                      method: 'POST'
                      uri: '@parameters(''kafkaPublisherUrl'')'
                      headers: { 'Content-Type': 'application/json' }
                      body: {
                        topic: 'inventory.release.command'
                        key: '@{body(''Parse_Order_Response'')[''orderId'']}'
                        value: '@{string(createObject(''orderId'', body(''Parse_Order_Response'')[''orderId''], ''orchestrationId'', workflow().run.name, ''issuedAt'', utcNow()))}'
                      }
                    }
                  }
                  Publish_OrderCancelled_OnShipFail: {
                    type: 'Http'
                    runAfter: {
                      Publish_InventoryReleaseCommand_OnShipFail: ['Succeeded']
                    }
                    inputs: {
                      method: 'POST'
                      uri: '@parameters(''kafkaPublisherUrl'')'
                      headers: { 'Content-Type': 'application/json' }
                      body: {
                        topic: 'order.cancelled'
                        key: '@{body(''Parse_Order_Response'')[''orderId'']}'
                        value: '@{string(createObject(''orderId'', body(''Parse_Order_Response'')[''orderId''], ''orchestrationId'', workflow().run.name, ''occurredAt'', utcNow()))}'
                      }
                    }
                  }
                }
              }
            }
          }
          else: {
            actions: {
              Publish_InventoryReleaseCommand_OnInvFail: {
                type: 'Http'
                runAfter: {}
                inputs: {
                  method: 'POST'
                  uri: '@parameters(''kafkaPublisherUrl'')'
                  headers: { 'Content-Type': 'application/json' }
                  body: {
                    topic: 'inventory.release.command'
                    key: '@{body(''Parse_Order_Response'')[''orderId'']}'
                    value: '@{string(createObject(''orderId'', body(''Parse_Order_Response'')[''orderId''], ''orchestrationId'', workflow().run.name, ''issuedAt'', utcNow()))}'
                  }
                }
              }
              Publish_OrderCancelled_OnInvFail: {
                type: 'Http'
                runAfter: {
                  Publish_InventoryReleaseCommand_OnInvFail: ['Succeeded']
                }
                inputs: {
                  method: 'POST'
                  uri: '@parameters(''kafkaPublisherUrl'')'
                  headers: { 'Content-Type': 'application/json' }
                  body: {
                    topic: 'order.cancelled'
                    key: '@{body(''Parse_Order_Response'')[''orderId'']}'
                    value: '@{string(createObject(''orderId'', body(''Parse_Order_Response'')[''orderId''], ''orchestrationId'', workflow().run.name, ''occurredAt'', utcNow()))}'
                  }
                }
              }
            }
          }
        }
      }
      outputs: {}
    }
    parameters: {
      kafkaPublisherUrl: { value: 'https://${kafkaBridge.properties.defaultHostName}/api/kafka/publish' }
      orderServiceUrl: { value: orderServiceUrl }
      callbackRegistrarUrl: { value: callbackRegistrarUrl }
    }
  }
}

// ── 3. InventoryReservedConsumer ──────────────────────────────
resource invReservedConsumer 'Microsoft.Logic/workflows@2019-05-01' = {
  name: '${prefix}-inv-reserved'
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {
    state: 'Enabled'
    definition: {
      '$schema': 'https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#'
      contentVersion: '1.0.0.0'
      parameters: {
        cosmosDbEndpoint: { type: 'String', defaultValue: '' }
      }
      triggers: {
        manual: {
          type: 'Request'
          kind: 'Http'
          inputs: {
            schema: {
              type: 'object'
              properties: {
                eventType: { type: 'string' }
                orderId: { type: 'string' }
                orchestrationId: { type: 'string' }
                reservedItems: { type: 'array' }
                occurredAt: { type: 'string' }
              }
            }
          }
        }
      }
      actions: {
        Parse_Kafka_Message: {
          type: 'ParseJson'
          runAfter: {}
          inputs: {
            content: '@triggerBody()'
            schema: {
              type: 'object'
              properties: {
                eventType: { type: 'string' }
                orderId: { type: 'string' }
                orchestrationId: { type: 'string' }
                reservedItems: { type: 'array' }
                occurredAt: { type: 'string' }
              }
            }
          }
        }
        Get_CallbackUrl: {
          type: 'Http'
          runAfter: { Parse_Kafka_Message: ['Succeeded'] }
          inputs: {
            method: 'GET'
            uri: '@{concat(parameters(''cosmosDbEndpoint''), ''dbs/sagadb/colls/CallbackUrls/docs/'', body(''Parse_Kafka_Message'')[''orchestrationId''])}'
            headers: {
              'x-ms-documentdb-partitionkey': '@{concat(''["'', body(''Parse_Kafka_Message'')[''orchestrationId''], ''"]'')}'
            }
            authentication: {
              type: 'ManagedServiceIdentity'
              audience: 'https://cosmos.azure.com'
            }
          }
        }
        Resume_Orchestrator: {
          type: 'Http'
          runAfter: { Get_CallbackUrl: ['Succeeded'] }
          inputs: {
            method: 'POST'
            uri: '@{body(''Get_CallbackUrl'')[''callbackUrl'']}'
            headers: { 'Content-Type': 'application/json' }
            body: {
              orchestrationId: '@{body(''Parse_Kafka_Message'')[''orchestrationId'']}'
              orderId: '@{body(''Parse_Kafka_Message'')[''orderId'']}'
              success: true
              eventType: 'InventoryReserved'
            }
          }
        }
        Delete_CallbackUrl: {
          type: 'Http'
          runAfter: { Resume_Orchestrator: ['Succeeded'] }
          inputs: {
            method: 'DELETE'
            uri: '@{concat(parameters(''cosmosDbEndpoint''), ''dbs/sagadb/colls/CallbackUrls/docs/'', body(''Parse_Kafka_Message'')[''orchestrationId''])}'
            headers: {
              'x-ms-documentdb-partitionkey': '@{concat(''["'', body(''Parse_Kafka_Message'')[''orchestrationId''], ''"]'')}'
            }
            authentication: {
              type: 'ManagedServiceIdentity'
              audience: 'https://cosmos.azure.com'
            }
          }
        }
      }
      outputs: {}
    }
    parameters: {
      cosmosDbEndpoint: { value: cosmosDb.properties.documentEndpoint }
    }
  }
}

resource invReservedCosmosRole 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2023-04-15' = {
  parent: cosmosDb
  name: guid(cosmosDb.id, invReservedConsumer.id, cosmosDataContributorRoleId)
  properties: {
    roleDefinitionId: '${cosmosDb.id}/sqlRoleDefinitions/${cosmosDataContributorRoleId}'
    principalId: invReservedConsumer.identity.principalId
    scope: cosmosDb.id
  }
  dependsOn: [callbackUrlsColl]
}

// ── 4. InventoryReservationFailedConsumer ─────────────────────
resource invFailedConsumer 'Microsoft.Logic/workflows@2019-05-01' = {
  name: '${prefix}-inv-failed'
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {
    state: 'Enabled'
    definition: {
      '$schema': 'https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#'
      contentVersion: '1.0.0.0'
      parameters: {
        cosmosDbEndpoint: { type: 'String', defaultValue: '' }
      }
      triggers: {
        manual: {
          type: 'Request'
          kind: 'Http'
          inputs: {
            schema: {
              type: 'object'
              properties: {
                eventType: { type: 'string' }
                orderId: { type: 'string' }
                orchestrationId: { type: 'string' }
                reason: { type: 'string' }
                occurredAt: { type: 'string' }
              }
            }
          }
        }
      }
      actions: {
        Parse_Kafka_Message: {
          type: 'ParseJson'
          runAfter: {}
          inputs: {
            content: '@triggerBody()'
            schema: {
              type: 'object'
              properties: {
                eventType: { type: 'string' }
                orderId: { type: 'string' }
                orchestrationId: { type: 'string' }
                reason: { type: 'string' }
                occurredAt: { type: 'string' }
              }
            }
          }
        }
        Get_CallbackUrl: {
          type: 'Http'
          runAfter: { Parse_Kafka_Message: ['Succeeded'] }
          inputs: {
            method: 'GET'
            uri: '@{concat(parameters(''cosmosDbEndpoint''), ''dbs/sagadb/colls/CallbackUrls/docs/'', body(''Parse_Kafka_Message'')[''orchestrationId''])}'
            headers: {
              'x-ms-documentdb-partitionkey': '@{concat(''["'', body(''Parse_Kafka_Message'')[''orchestrationId''], ''"]'')}'
            }
            authentication: {
              type: 'ManagedServiceIdentity'
              audience: 'https://cosmos.azure.com'
            }
          }
        }
        Resume_Orchestrator: {
          type: 'Http'
          runAfter: { Get_CallbackUrl: ['Succeeded'] }
          inputs: {
            method: 'POST'
            uri: '@{body(''Get_CallbackUrl'')[''callbackUrl'']}'
            headers: { 'Content-Type': 'application/json' }
            body: {
              orchestrationId: '@{body(''Parse_Kafka_Message'')[''orchestrationId'']}'
              orderId: '@{body(''Parse_Kafka_Message'')[''orderId'']}'
              success: false
              reason: '@{body(''Parse_Kafka_Message'')[''reason'']}'
              eventType: 'InventoryReservationFailed'
            }
          }
        }
        Delete_CallbackUrl: {
          type: 'Http'
          runAfter: { Resume_Orchestrator: ['Succeeded'] }
          inputs: {
            method: 'DELETE'
            uri: '@{concat(parameters(''cosmosDbEndpoint''), ''dbs/sagadb/colls/CallbackUrls/docs/'', body(''Parse_Kafka_Message'')[''orchestrationId''])}'
            headers: {
              'x-ms-documentdb-partitionkey': '@{concat(''["'', body(''Parse_Kafka_Message'')[''orchestrationId''], ''"]'')}'
            }
            authentication: {
              type: 'ManagedServiceIdentity'
              audience: 'https://cosmos.azure.com'
            }
          }
        }
      }
      outputs: {}
    }
    parameters: {
      cosmosDbEndpoint: { value: cosmosDb.properties.documentEndpoint }
    }
  }
}

resource invFailedCosmosRole 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2023-04-15' = {
  parent: cosmosDb
  name: guid(cosmosDb.id, invFailedConsumer.id, cosmosDataContributorRoleId)
  properties: {
    roleDefinitionId: '${cosmosDb.id}/sqlRoleDefinitions/${cosmosDataContributorRoleId}'
    principalId: invFailedConsumer.identity.principalId
    scope: cosmosDb.id
  }
  dependsOn: [callbackUrlsColl]
}

// ── 5. ShippingScheduledConsumer ──────────────────────────────
resource shipConsumer 'Microsoft.Logic/workflows@2019-05-01' = {
  name: '${prefix}-ship-sched'
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {
    state: 'Enabled'
    definition: {
      '$schema': 'https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#'
      contentVersion: '1.0.0.0'
      parameters: {
        cosmosDbEndpoint: { type: 'String', defaultValue: '' }
      }
      triggers: {
        manual: {
          type: 'Request'
          kind: 'Http'
          inputs: {
            schema: {
              type: 'object'
              properties: {
                eventType: { type: 'string' }
                orderId: { type: 'string' }
                orchestrationId: { type: 'string' }
                shippingId: { type: 'string' }
                occurredAt: { type: 'string' }
              }
            }
          }
        }
      }
      actions: {
        Parse_Kafka_Message: {
          type: 'ParseJson'
          runAfter: {}
          inputs: {
            content: '@triggerBody()'
            schema: {
              type: 'object'
              properties: {
                eventType: { type: 'string' }
                orderId: { type: 'string' }
                orchestrationId: { type: 'string' }
                shippingId: { type: 'string' }
                occurredAt: { type: 'string' }
              }
            }
          }
        }
        Get_CallbackUrl: {
          type: 'Http'
          runAfter: { Parse_Kafka_Message: ['Succeeded'] }
          inputs: {
            method: 'GET'
            uri: '@{concat(parameters(''cosmosDbEndpoint''), ''dbs/sagadb/colls/CallbackUrls/docs/'', body(''Parse_Kafka_Message'')[''orchestrationId''])}'
            headers: {
              'x-ms-documentdb-partitionkey': '@{concat(''["'', body(''Parse_Kafka_Message'')[''orchestrationId''], ''"]'')}'
            }
            authentication: {
              type: 'ManagedServiceIdentity'
              audience: 'https://cosmos.azure.com'
            }
          }
        }
        Resume_Orchestrator: {
          type: 'Http'
          runAfter: { Get_CallbackUrl: ['Succeeded'] }
          inputs: {
            method: 'POST'
            uri: '@{body(''Get_CallbackUrl'')[''callbackUrl'']}'
            headers: { 'Content-Type': 'application/json' }
            body: {
              orchestrationId: '@{body(''Parse_Kafka_Message'')[''orchestrationId'']}'
              orderId: '@{body(''Parse_Kafka_Message'')[''orderId'']}'
              shippingId: '@{body(''Parse_Kafka_Message'')[''shippingId'']}'
              success: true
              eventType: 'ShippingScheduled'
            }
          }
        }
        Delete_CallbackUrl: {
          type: 'Http'
          runAfter: { Resume_Orchestrator: ['Succeeded'] }
          inputs: {
            method: 'DELETE'
            uri: '@{concat(parameters(''cosmosDbEndpoint''), ''dbs/sagadb/colls/CallbackUrls/docs/'', body(''Parse_Kafka_Message'')[''orchestrationId''])}'
            headers: {
              'x-ms-documentdb-partitionkey': '@{concat(''["'', body(''Parse_Kafka_Message'')[''orchestrationId''], ''"]'')}'
            }
            authentication: {
              type: 'ManagedServiceIdentity'
              audience: 'https://cosmos.azure.com'
            }
          }
        }
      }
      outputs: {}
    }
    parameters: {
      cosmosDbEndpoint: { value: cosmosDb.properties.documentEndpoint }
    }
  }
}

resource shipCosmosRole 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2023-04-15' = {
  parent: cosmosDb
  name: guid(cosmosDb.id, shipConsumer.id, cosmosDataContributorRoleId)
  properties: {
    roleDefinitionId: '${cosmosDb.id}/sqlRoleDefinitions/${cosmosDataContributorRoleId}'
    principalId: shipConsumer.identity.principalId
    scope: cosmosDb.id
  }
  dependsOn: [callbackUrlsColl]
}

// ── Kafka Bridge Function App ───────────────────────────────
// 新規: func-ketana-ext2-kafka-bridge
// KafkaPublisher (HTTP→Kafka) / KafkaConsumer (Kafka→Logic App HTTP)

var bridgeFuncName    = 'func-ketana-ext2-kafka-bridge'
var bridgeStorageName = 'stketanaext2bridge'
var bridgePlanName    = 'asp-ketana-ext2-bridge'
// Storage RBAC ロール定義 ID（Azure 組み込み）
var storageBlobDataOwnerRoleId        = 'b7e6dc63-0a8d-42c5-b61d-b5df40baf8f0'
var storageQueueDataContributorRoleId = '974c5e8b-45b9-4653-ba55-5f855dd0fb88'
var storageTableDataContributorRoleId = '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3'

resource bridgeStorage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: bridgeStorageName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    allowSharedKeyAccess: false
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
  }
}

resource bridgePlan 'Microsoft.Web/serverfarms@2023-01-01' = {
  name: bridgePlanName
  location: location
  sku: { name: 'B1', tier: 'Basic' }
  kind: 'linux'
  properties: { reserved: true }
}

resource kafkaBridge 'Microsoft.Web/sites@2023-01-01' = {
  name: bridgeFuncName
  location: location
  kind: 'functionapp,linux'
  identity: { type: 'SystemAssigned' }
  properties: {
    serverFarmId: bridgePlan.id
    httpsOnly: true
    siteConfig: {
      linuxFxVersion: 'Java|21'
      appSettings: [
        { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'java' }
        { name: 'AzureWebJobsStorage__accountName', value: bridgeStorage.name }
        { name: 'AzureWebJobsStorage__credential', value: 'managedidentity' }
        { name: 'KAFKA_BOOTSTRAP_SERVERS', value: kafkaBootstrapServers }
        { name: 'LOGIC_APP_INV_RESERVED_URL', value: '' }
        { name: 'LOGIC_APP_INV_FAILED_URL', value: '' }
        { name: 'LOGIC_APP_SHIP_SCHED_URL', value: '' }
      ]
    }
  }
}

resource bridgeFuncBlobRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(bridgeStorage.id, kafkaBridge.id, storageBlobDataOwnerRoleId)
  scope: bridgeStorage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataOwnerRoleId)
    principalId: kafkaBridge.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource bridgeFuncQueueRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(bridgeStorage.id, kafkaBridge.id, storageQueueDataContributorRoleId)
  scope: bridgeStorage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageQueueDataContributorRoleId)
    principalId: kafkaBridge.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource bridgeFuncTableRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(bridgeStorage.id, kafkaBridge.id, storageTableDataContributorRoleId)
  scope: bridgeStorage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageTableDataContributorRoleId)
    principalId: kafkaBridge.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// ── Outputs ────────────────────────────────────────────
output callbackRegName string = callbackReg.name
output orderSagaName string = orderSaga.name
output invReservedConsumerName string = invReservedConsumer.name
output invFailedConsumerName string = invFailedConsumer.name
output shipConsumerName string = shipConsumer.name
output cosmosDbEndpoint string = cosmosDb.properties.documentEndpoint
output kafkaBridgeName string = kafkaBridge.name
output kafkaBridgeUrl string = 'https://${kafkaBridge.properties.defaultHostName}/api/kafka/publish'
