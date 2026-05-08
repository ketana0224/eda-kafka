package com.example.saga.model;

/**
 * order.cancelled トピックに発行するコマンド（補償）。
 * Orchestrator が在庫不足・タイムアウト時に発行し、OrderService Consumer が CANCELLED に更新する。
 */
public record OrderCancelledCommand(
    String eventType,
    String orderId,
    String reason,
    String occurredAt
) {}
