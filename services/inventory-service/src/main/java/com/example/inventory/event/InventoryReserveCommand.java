package com.example.inventory.event;

import java.util.List;

/**
 * inventory.reserve.command トピックから受信するコマンド。
 * Durable Functions Orchestrator が発行する。
 * orchestrationId をレスポンスイベントにパススルーすることで相関を実現する。
 */
public record InventoryReserveCommand(
    String orderId,
    List<OrderItemDto> items,
    String orchestrationId,
    String issuedAt
) {
    public record OrderItemDto(String productId, int quantity) {}
}
