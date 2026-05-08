package com.example.saga.model;

/**
 * OrderService の POST /api/orders レスポンスボディ。
 */
public record CreateOrderResponse(String orderId, String status) {}
