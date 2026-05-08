package com.example.inventory.service;

import com.example.inventory.event.*;
import com.example.inventory.event.InventoryReservationFailedEvent.ShortageItemDto;
import com.example.inventory.event.InventoryReservedEvent.ReservedItemDto;
import com.example.inventory.kafka.InventoryProducer;
import com.example.inventory.model.Product;
import com.example.inventory.model.Reservation;
import com.example.inventory.model.ReservationStatus;
import com.example.inventory.repository.ProductRepository;
import com.example.inventory.repository.ReservationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final ProductRepository productRepo;
    private final ReservationRepository reservationRepo;
    private final InventoryProducer producer;

    public InventoryService(ProductRepository productRepo,
                            ReservationRepository reservationRepo,
                            InventoryProducer producer) {
        this.productRepo = productRepo;
        this.reservationRepo = reservationRepo;
        this.producer = producer;
    }

    /**
     * 在庫引当処理。
     * 全商品の在庫が充足している場合のみ引当を確定し、inventory.reserved を発行する。
     * いずれか 1 商品でも不足している場合は引当を行わず inventory.reservation.failed を発行する。
     */
    @Transactional
    public void reserveInventory(InventoryReserveCommand cmd) throws Exception {
        // 冪等性チェック：同一 orderId が既に RESERVED なら再送とみなしてスキップ
        if (reservationRepo.existsByOrderIdAndStatus(cmd.orderId(), ReservationStatus.RESERVED)) {
            log.info("Idempotent skip: orderId={} already RESERVED", cmd.orderId());
            return;
        }

        List<ShortageItemDto> shortages = new ArrayList<>();
        List<ReservedItemDto> reserved = new ArrayList<>();

        // 在庫確認フェーズ（ロックを取得してから検証）
        for (var item : cmd.items()) {
            Product p = productRepo.findByIdForUpdate(item.productId())
                .orElse(null);
            if (p == null || p.getStockQuantity() < item.quantity()) {
                int available = p != null ? p.getStockQuantity() : 0;
                shortages.add(new ShortageItemDto(item.productId(), item.quantity(), available));
            }
        }

        if (!shortages.isEmpty()) {
            var failedEvent = new InventoryReservationFailedEvent(
                "InventoryReservationFailed",
                cmd.orderId(),
                cmd.orchestrationId(),
                "Stock shortage",
                shortages,
                Instant.now().toString()
            );
            producer.sendReservationFailed(failedEvent);
            log.warn("Reservation failed: orderId={} shortages={}", cmd.orderId(), shortages.size());
            return;
        }

        // 引当確定フェーズ
        for (var item : cmd.items()) {
            Product p = productRepo.findByIdForUpdate(item.productId()).orElseThrow();
            p.setStockQuantity(p.getStockQuantity() - item.quantity());
            productRepo.save(p);

            Reservation r = new Reservation();
            r.setOrderId(cmd.orderId());
            r.setProductId(item.productId());
            r.setQuantity(item.quantity());
            r.setStatus(ReservationStatus.RESERVED);
            r.setCreatedAt(Instant.now());
            reservationRepo.save(r);

            reserved.add(new ReservedItemDto(item.productId(), item.quantity()));
        }

        var reservedEvent = new InventoryReservedEvent(
            "InventoryReserved", cmd.orderId(), cmd.orchestrationId(), reserved, Instant.now().toString()
        );
        producer.sendReserved(reservedEvent);
        log.info("Reservation succeeded: orderId={}", cmd.orderId());
    }

    /**
     * 在庫解放処理（補償トランザクション）。
     * RESERVED 状態の引当レコードを RELEASED に変更し、在庫数を戻す。
     */
    @Transactional
    public void releaseInventory(InventoryReleaseCommand cmd) {
        List<Reservation> reservations =
            reservationRepo.findByOrderIdAndStatus(cmd.orderId(), ReservationStatus.RESERVED);

        for (Reservation r : reservations) {
            productRepo.findByIdForUpdate(r.getProductId()).ifPresent(p -> {
                p.setStockQuantity(p.getStockQuantity() + r.getQuantity());
                productRepo.save(p);
            });
            r.setStatus(ReservationStatus.RELEASED);
            reservationRepo.save(r);
        }
        log.info("Inventory released: orderId={} count={}", cmd.orderId(), reservations.size());
    }
}
