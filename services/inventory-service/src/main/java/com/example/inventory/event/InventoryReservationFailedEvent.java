package com.example.inventory.event;

import java.util.List;

/**
 * inventory.reservation.failed トピックに発行するイベント。
 * 在庫不足時に Orchestrator へ通知する（補償トランザクション開始）。
 * orchestrationId を包むことで KafkaConsumer Function が waitForExternalEvent を解除できる。
 */
public record InventoryReservationFailedEvent(
    String eventType,
    String orderId,
    String orchestrationId,
    String reason,
    List<ShortageItemDto> shortageItems,
    String occurredAt
) {
    public record ShortageItemDto(String productId, int requested, int available) {}
}
