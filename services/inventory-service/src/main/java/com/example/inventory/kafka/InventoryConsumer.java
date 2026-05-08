package com.example.inventory.kafka;

import com.example.inventory.event.InventoryReleaseCommand;
import com.example.inventory.event.InventoryReserveCommand;
import com.example.inventory.service.InventoryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    private final InventoryService service;
    private final ObjectMapper mapper;

    public InventoryConsumer(InventoryService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "inventory.reserve.command", groupId = "inventory-service-group")
    public void handleReserveCommand(ConsumerRecord<String, String> record) throws Exception {
        InventoryReserveCommand cmd = mapper.readValue(record.value(), InventoryReserveCommand.class);
        log.info("Reserve command received: orderId={}", cmd.orderId());
        service.reserveInventory(cmd);
    }

    @KafkaListener(topics = "inventory.release.command", groupId = "inventory-service-group")
    public void handleReleaseCommand(ConsumerRecord<String, String> record) throws Exception {
        InventoryReleaseCommand cmd = mapper.readValue(record.value(), InventoryReleaseCommand.class);
        log.info("Release command received: orderId={}", cmd.orderId());
        service.releaseInventory(cmd);
    }
}
