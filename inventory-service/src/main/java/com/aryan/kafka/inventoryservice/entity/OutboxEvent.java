package com.aryan.kafka.inventoryservice.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "topic_name", nullable = false)
    private String topicName;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Lob
    @Column(name = "payload", nullable = false,columnDefinition = "TEXT")
    private String payload;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "published_at")
    private LocalDateTime publishedAt;

    public OutboxEvent() {
    }

    public OutboxEvent(
            String eventId,
            Long orderId,
            String topicName,
            String eventType,
            String payload,
            String status,
            LocalDateTime createdAt
    ) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.topicName = topicName;
        this.eventType = eventType;
        this.payload = payload;
        this.status = status;
        this.createdAt = createdAt;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public String getStatus() {
        return status;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public LocalDateTime getPublishedAt() {
        return publishedAt;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setPublishedAt(LocalDateTime publishedAt) {
        this.publishedAt = publishedAt;
    }
}