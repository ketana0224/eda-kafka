package com.example.order.event;

/**
 * order.cancelled トピックから受信するイベント。
 * Durable Functions Orchestrator が在庫引当失敗時に発行する（補償）。
 */
public record OrderCancelledEvent(
    String eventType,
    String orderId,
    String reason,
    String occurredAt
) {}
