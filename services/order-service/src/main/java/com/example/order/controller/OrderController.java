package com.example.order.controller;

import com.example.order.model.Order;
import com.example.order.model.OrderItemDto;
import com.example.order.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * OrderService REST API
 *
 * POST /api/orders  - 注文作成（Durable Functions Orchestrator の Activity から呼ばれる）
 * GET  /api/orders/{orderId} - 注文ステータス取得
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService service;

    public OrderController(OrderService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> createOrder(@RequestBody CreateOrderRequest req) throws Exception {
        Order order = service.createOrder(
            req.customerId(), req.shippingAddress(), req.items(), req.totalAmount()
        );
        return ResponseEntity
            .created(URI.create("/api/orders/" + order.getOrderId()))
            .body(Map.of(
                "orderId", order.getOrderId(),
                "status", order.getStatus().name()
            ));
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<?> getOrder(@PathVariable String orderId) {
        Order order = service.getOrder(orderId);
        if (order == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(Map.of(
            "orderId", order.getOrderId(),
            "customerId", order.getCustomerId(),
            "status", order.getStatus().name(),
            "totalAmount", order.getTotalAmount(),
            "shippingAddress", order.getShippingAddress(),
            "shippingId", order.getShippingId() != null ? order.getShippingId() : "",
            "createdAt", order.getCreatedAt().toString(),
            "updatedAt", order.getUpdatedAt().toString()
        ));
    }

    public record CreateOrderRequest(
        String customerId,
        String shippingAddress,
        List<OrderItemDto> items,
        long totalAmount
    ) {}
}
