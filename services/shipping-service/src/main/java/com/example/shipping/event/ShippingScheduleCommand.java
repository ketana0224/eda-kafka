package com.example.shipping.event;

/**
 * shipping.schedule.command トピックから受信するコマンド。
 * Durable Functions Orchestrator が在庫引当成功後に発行する。
 * orchestrationId をレスポンスイベントにパススルーすることで相関を実現する。
 */
public record ShippingScheduleCommand(
    String orderId,
    String shippingAddress,
    String orchestrationId,
    String issuedAt
) {}
