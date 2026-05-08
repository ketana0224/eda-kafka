package com.example.saga.model;

/**
 * inventory.release.command トピックに発行するコマンド（補償）。
 */
public record InventoryReleaseCommand(
    String orderId,
    String issuedAt
) {}
