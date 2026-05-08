package com.example.inventory.kafka;

import com.example.inventory.event.InventoryReservationFailedEvent;
import com.example.inventory.event.InventoryReservedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper;

    public InventoryProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
    }

    public void sendReserved(InventoryReservedEvent event) throws Exception {
        String json = mapper.writeValueAsString(event);
        kafkaTemplate.send("inventory.reserved", event.orderId(), json);
    }

    public void sendReservationFailed(InventoryReservationFailedEvent event) throws Exception {
        String json = mapper.writeValueAsString(event);
        kafkaTemplate.send("inventory.reservation.failed", event.orderId(), json);
    }
}
