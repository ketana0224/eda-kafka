package com.example.saga.model;

import java.util.List;

/**
 * inventory.reservation.failed トピックから受信するイベントの JSON マッピング。
 * orchestrationId を使って Orchestrator インスタンスと相関させる。
 */
public record InventoryReservationFailedMsg(
    String eventType,
    String orderId,
    String orchestrationId,
    String reason,
    List<ShortageItemDto> shortageItems,
    String occurredAt
) {
    public record ShortageItemDto(String productId, int requested, int available) {}
}
