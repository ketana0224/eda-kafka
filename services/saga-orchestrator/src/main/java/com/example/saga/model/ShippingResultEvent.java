package com.example.saga.model;

/**
 * KafkaConsumerFunction が waitForExternalEvent("ShippingResult") に渡すペイロード。
 * shipping.scheduled イベントを受信した際に使用する。
 */
public record ShippingResultEvent(boolean success, String shippingId) {}
