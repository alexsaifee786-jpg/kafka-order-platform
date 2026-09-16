package com.aryan.kafka.orderservice.event;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;

public class InventoryReservationFailedEvent {

    private String eventId;
    private String sourceEventId;
    private Long orderId;
    private Long productId;
    private Integer quantity;
    private String status;
    private String reason;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private LocalDateTime failedAt;

    public InventoryReservationFailedEvent() {
    }

    public InventoryReservationFailedEvent(
            String eventId,
            String sourceEventId,
            Long orderId,
            Long productId,
            Integer quantity,
            String status,
            String reason,
            LocalDateTime failedAt) {

        this.eventId = eventId;
        this.sourceEventId = sourceEventId;
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.status = status;
        this.reason = reason;
        this.failedAt = failedAt;
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

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public LocalDateTime getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(LocalDateTime failedAt) {
        this.failedAt = failedAt;
    }
}