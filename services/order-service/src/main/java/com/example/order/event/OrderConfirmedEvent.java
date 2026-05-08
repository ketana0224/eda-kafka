package com.example.order.event;

/**
 * order.confirmed トピックから受信するイベント。
 * Durable Functions Orchestrator が Saga 完了時に発行する。
 */
public record OrderConfirmedEvent(
    String eventType,
    String orderId,
    String shippingId,
    String occurredAt
) {}
