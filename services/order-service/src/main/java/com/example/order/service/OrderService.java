package com.example.order.service;

import com.example.order.model.Order;
import com.example.order.model.OrderItemDto;
import com.example.order.model.OrderStatus;
import com.example.order.repository.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository repo;
    private final ObjectMapper mapper;

    public OrderService(OrderRepository repo, ObjectMapper mapper) {
        this.repo = repo;
        this.mapper = mapper;
    }

    @Transactional
    public Order createOrder(String customerId, String shippingAddress,
                             List<OrderItemDto> items,
                             long totalAmount) throws Exception {
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        Order order = new Order();
        order.setOrderId(orderId);
        order.setCustomerId(customerId);
        order.setStatus(OrderStatus.PENDING);
        order.setShippingAddress(shippingAddress);
        order.setTotalAmount(totalAmount);
        order.setItemsJson(mapper.writeValueAsString(items));
        order.setCreatedAt(Instant.now());
        order.setUpdatedAt(Instant.now());
        repo.save(order);

        return order;
    }

    @Transactional
    public void confirmOrder(String orderId, String shippingId) {
        repo.findById(orderId).ifPresent(order -> {
            order.setStatus(OrderStatus.CONFIRMED);
            order.setShippingId(shippingId);
            order.setUpdatedAt(Instant.now());
            repo.save(order);
        });
    }

    @Transactional
    public void cancelOrder(String orderId) {
        repo.findById(orderId).ifPresent(order -> {
            order.setStatus(OrderStatus.CANCELLED);
            order.setUpdatedAt(Instant.now());
            repo.save(order);
        });
    }

    public Order getOrder(String orderId) {
        return repo.findById(orderId).orElse(null);
    }
}
