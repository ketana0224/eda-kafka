package com.example.saga.model;

/**
 * shipping.scheduled トピックから受信するイベントの JSON マッピング。
 * orchestrationId を使って Orchestrator インスタンスと相関させる。
 */
public record ShippingScheduledMsg(
    String eventType,
    String orderId,
    String orchestrationId,
    String shippingId,
    String scheduledDate,
    String occurredAt
) {}
