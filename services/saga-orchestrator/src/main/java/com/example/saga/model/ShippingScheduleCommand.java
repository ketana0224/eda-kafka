package com.example.saga.model;

/**
 * shipping.schedule.command トピックに発行するコマンド。
 * orchestrationId を含めることで ShippingService がレスポンスに折り返す。
 */
public record ShippingScheduleCommand(
    String orderId,
    String shippingAddress,
    String orchestrationId,
    String issuedAt
) {}
