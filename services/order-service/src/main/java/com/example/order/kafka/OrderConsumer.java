package com.example.order.kafka;

import com.example.order.event.OrderCancelledEvent;
import com.example.order.event.OrderConfirmedEvent;
import com.example.order.service.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final OrderService service;
    private final ObjectMapper mapper;

    public OrderConsumer(OrderService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "order.confirmed", groupId = "order-service-group")
    public void handleOrderConfirmed(ConsumerRecord<String, String> record) throws Exception {
        OrderConfirmedEvent event = mapper.readValue(record.value(), OrderConfirmedEvent.class);
        log.info("Order confirmed: orderId={} shippingId={}", event.orderId(), event.shippingId());
        service.confirmOrder(event.orderId(), event.shippingId());
    }

    @KafkaListener(topics = "order.cancelled", groupId = "order-service-group")
    public void handleOrderCancelled(ConsumerRecord<String, String> record) throws Exception {
        OrderCancelledEvent event = mapper.readValue(record.value(), OrderCancelledEvent.class);
        log.info("Order cancelled: orderId={} reason={}", event.orderId(), event.reason());
        service.cancelOrder(event.orderId());
    }
}
