package com.example.shipping.service;

import com.example.shipping.event.ShippingScheduleCommand;
import com.example.shipping.event.ShippingScheduledEvent;
import com.example.shipping.kafka.ShippingProducer;
import com.example.shipping.model.Shipment;
import com.example.shipping.model.ShipmentStatus;
import com.example.shipping.repository.ShipmentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Service
public class ShippingService {

    private static final Logger log = LoggerFactory.getLogger(ShippingService.class);

    private final ShipmentRepository repo;
    private final ShippingProducer producer;

    public ShippingService(ShipmentRepository repo, ShippingProducer producer) {
        this.repo = repo;
        this.producer = producer;
    }

    /**
     * 配送スケジュール登録。
     * 冪等性: 同一 orderId が既に存在する場合は再送とみなしスキップする。
     */
    @Transactional
    public void scheduleShipping(ShippingScheduleCommand cmd) throws Exception {
        // 冪等性チェック
        if (repo.findByOrderId(cmd.orderId()).isPresent()) {
            log.info("Idempotent skip: orderId={} already scheduled", cmd.orderId());
            return;
        }

        String shippingId = "SHP-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        Instant scheduledDate = Instant.now().plus(3, ChronoUnit.DAYS);

        Shipment shipment = new Shipment();
        shipment.setShippingId(shippingId);
        shipment.setOrderId(cmd.orderId());
        shipment.setShippingAddress(cmd.shippingAddress());
        shipment.setStatus(ShipmentStatus.SCHEDULED);
        shipment.setScheduledDate(scheduledDate);
        shipment.setCreatedAt(Instant.now());
        repo.save(shipment);

        ShippingScheduledEvent event = new ShippingScheduledEvent(
            "ShippingScheduled",
            cmd.orderId(),
            cmd.orchestrationId(),
            shippingId,
            scheduledDate.toString(),
            Instant.now().toString()
        );
        producer.sendShippingScheduled(event);
        log.info("Shipping scheduled: orderId={} shippingId={}", cmd.orderId(), shippingId);
    }
}
