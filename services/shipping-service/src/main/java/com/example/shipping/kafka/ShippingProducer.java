package com.example.shipping.kafka;

import com.example.shipping.event.ShippingScheduledEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ShippingProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper;

    public ShippingProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
    }

    public void sendShippingScheduled(ShippingScheduledEvent event) throws Exception {
        String json = mapper.writeValueAsString(event);
        kafkaTemplate.send("shipping.scheduled", event.orderId(), json);
    }
}
