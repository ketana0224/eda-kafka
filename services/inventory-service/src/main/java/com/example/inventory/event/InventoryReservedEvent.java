package com.example.inventory.event;

import java.util.List;

/**
 * inventory.reserved トピックに発行するイベント。
 * 在庫引当成功時に Orchestrator へ通知する。
 * orchestrationId を包むことで KafkaConsumer Function が waitForExternalEvent を解除できる。
 */
public record InventoryReservedEvent(
    String eventType,
    String orderId,
    String orchestrationId,
    List<ReservedItemDto> reservedItems,
    String occurredAt
) {
    public record ReservedItemDto(String productId, int quantity) {}
}
