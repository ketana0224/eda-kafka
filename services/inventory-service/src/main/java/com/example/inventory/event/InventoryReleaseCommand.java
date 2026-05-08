package com.example.inventory.event;

/**
 * inventory.release.command トピックから受信するコマンド。
 * Orchestrator が補償トランザクションとして在庫解放を指示する。
 */
public record InventoryReleaseCommand(
    String orderId,
    String issuedAt
) {}
