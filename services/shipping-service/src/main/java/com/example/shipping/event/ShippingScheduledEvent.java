package com.example.shipping.event;

/**
 * shipping.scheduled トピックに発行するイベント。
 * 配送スケジュール登録完了を Orchestrator へ通知する。
 * orchestrationId を包むことで KafkaConsumer Function が waitForExternalEvent を解除できる。
 */
public record ShippingScheduledEvent(
    String eventType,
    String orderId,
    String orchestrationId,
    String shippingId,
    String scheduledDate,
    String occurredAt
) {}
