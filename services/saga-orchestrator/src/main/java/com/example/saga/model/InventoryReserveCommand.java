package com.example.saga.model;

import java.util.List;

/**
 * inventory.reserve.command トピックに発行するコマンド。
 * orchestrationId を含めることで InventoryService がレスポンスに折り返す。
 */
public record InventoryReserveCommand(
    String orderId,
    List<SagaInput.OrderItemDto> items,
    String orchestrationId,
    String issuedAt
) {}
