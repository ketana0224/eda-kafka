package com.example.saga.model;

/**
 * order.confirmed トピックに発行するコマンド。
 * Orchestrator が Saga 完了時に発行し、OrderService Consumer が CONFIRMED に更新する。
 */
public record OrderConfirmedCommand(
    String eventType,
    String orderId,
    String shippingId,
    String occurredAt
) {}
