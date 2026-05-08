package com.example.shipping.kafka;

import com.example.shipping.event.ShippingScheduleCommand;
import com.example.shipping.service.ShippingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ShippingConsumer {

    private static final Logger log = LoggerFactory.getLogger(ShippingConsumer.class);

    private final ShippingService service;
    private final ObjectMapper mapper;

    public ShippingConsumer(ShippingService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "shipping.schedule.command", groupId = "shipping-service-group")
    public void handleScheduleCommand(ConsumerRecord<String, String> record) throws Exception {
        ShippingScheduleCommand cmd = mapper.readValue(record.value(), ShippingScheduleCommand.class);
        log.info("Schedule command received: orderId={}", cmd.orderId());
        service.scheduleShipping(cmd);
    }
}
