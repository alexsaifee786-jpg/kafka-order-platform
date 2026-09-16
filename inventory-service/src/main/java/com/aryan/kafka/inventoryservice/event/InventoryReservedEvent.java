package com.aryan.kafka.inventoryservice.event;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;

public class InventoryReservedEvent {

    private String eventId;
    private String sourceEventId;
    private Long orderId;
    private Long productId;
    private Integer quantity;
    private String status;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private LocalDateTime reservedAt;

    public InventoryReservedEvent() {
    }

    public InventoryReservedEvent(
            String eventId,
            String sourceEventId,
            Long orderId,
            Long productId,
            Integer quantity,
            String status,
            LocalDateTime reservedAt
    ) {
        this.eventId = eventId;
        this.sourceEventId = sourceEventId;
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.status = status;
        this.reservedAt = reservedAt;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getSourceEventId() {
        return sourceEventId;
    }

    public void setSourceEventId(String sourceEventId) {
        this.sourceEventId = sourceEventId;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getReservedAt() {
        return reservedAt;
    }

    public void setReservedAt(LocalDateTime reservedAt) {
        this.reservedAt = reservedAt;
    }
}