package com.example.saga.model;

import java.util.List;

/**
 * inventory.reserved トピックから受信するイベントの JSON マッピング。
 * orchestrationId を使って Orchestrator インスタンスと相関させる。
 */
public record InventoryReservedMsg(
    String eventType,
    String orderId,
    String orchestrationId,
    List<ReservedItemDto> reservedItems,
    String occurredAt
) {
    public record ReservedItemDto(String productId, int quantity) {}
}
