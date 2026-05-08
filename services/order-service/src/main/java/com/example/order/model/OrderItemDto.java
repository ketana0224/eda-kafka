package com.example.order.model;

/**
 * 注文明細 DTO。
 * OrderController のリクエストボディおよび OrderService のビジネスロジックで使用する。
 */
public record OrderItemDto(String productId, int quantity) {}
