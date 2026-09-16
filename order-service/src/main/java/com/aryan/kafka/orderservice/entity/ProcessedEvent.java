package com.aryan.kafka.orderservice.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "topic_name", nullable = false)
    private String topicName;

    @Column(name = "partition_id", nullable = false)
    private Integer partitionId;

    @Column(name = "kafka_offset", nullable = false)
    private Long kafkaOffset;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;

    public ProcessedEvent() {
    }

    public ProcessedEvent(
            String eventId,
            Long orderId,
            String topicName,
            Integer partitionId,
            Long kafkaOffset,
            LocalDateTime processedAt) {

        this.eventId = eventId;
        this.orderId = orderId;
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.kafkaOffset = kafkaOffset;
        this.processedAt = processedAt;
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

    public Integer getPartitionId() {
        return partitionId;
    }

    public Long getKafkaOffset() {
        return kafkaOffset;
    }

    public LocalDateTime getProcessedAt() {
        return processedAt;
    }
}