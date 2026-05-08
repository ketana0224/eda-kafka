package com.example.saga.model;

import java.util.List;

/**
 * HTTP Trigger が受け取るリクエストボディ兼 Orchestrator への入力。
 */
public record SagaInput(
    String customerId,
    String shippingAddress,
    List<OrderItemDto> items,
    long totalAmount
) {
    public record OrderItemDto(String productId, int quantity) {}
}
