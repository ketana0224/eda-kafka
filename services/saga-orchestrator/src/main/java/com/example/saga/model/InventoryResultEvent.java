package com.example.saga.model;

/**
 * KafkaConsumerFunction が waitForExternalEvent("InventoryResult") に渡すペイロード。
 * inventory.reserved / inventory.reservation.failed どちらのイベントでも共通で使用する。
 */
public record InventoryResultEvent(boolean success, String reason) {}
