package com.example.shipping.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "shipments",
       indexes = @Index(name = "idx_shipments_order_id", columnList = "order_id"))
public class Shipment {

    @Id
    @Column(name = "shipping_id", length = 50)
    private String shippingId;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "shipping_address", nullable = false)
    private String shippingAddress;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private ShipmentStatus status;

    @Column(name = "scheduled_date")
    private Instant scheduledDate;

    @Column(name = "created_at")
    private Instant createdAt;

    public Shipment() {}

    public String getShippingId() { return shippingId; }
    public void setShippingId(String shippingId) { this.shippingId = shippingId; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getShippingAddress() { return shippingAddress; }
    public void setShippingAddress(String shippingAddress) { this.shippingAddress = shippingAddress; }

    public ShipmentStatus getStatus() { return status; }
    public void setStatus(ShipmentStatus status) { this.status = status; }

    public Instant getScheduledDate() { return scheduledDate; }
    public void setScheduledDate(Instant scheduledDate) { this.scheduledDate = scheduledDate; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}
