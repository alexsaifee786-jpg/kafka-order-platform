package com.aryan.kafka.inventoryservice.event;

import java.math.BigDecimal;
import java.time.Instant;

public class OrderCreatedEvent {

    private String eventId;
    private Long orderId;
    private Long productId;
    private Integer quantity;
    private BigDecimal amount;
    private String status;
    private Instant occurredAt;

    public OrderCreatedEvent() {
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }

    @Override
    public String toString() {
        return "OrderCreatedEvent{" +
                "eventId='" + eventId + '\'' +
                ", orderId=" + orderId +
                ", productId=" + productId +
                ", quantity=" + quantity +
                ", amount=" + amount +
                ", status='" + status + '\'' +
                ", occurredAt=" + occurredAt +
                '}';
    }
}