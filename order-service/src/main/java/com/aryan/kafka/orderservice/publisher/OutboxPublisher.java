package com.aryan.kafka.orderservice.publisher;

import com.aryan.kafka.orderservice.entity.OutboxEvent;
import com.aryan.kafka.orderservice.event.OrderCreatedEvent;
import com.aryan.kafka.orderservice.producer.OrderEventProducer;
import com.aryan.kafka.orderservice.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {

    private static final Logger log =
            LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final OrderEventProducer orderEventProducer;
    private final ObjectMapper objectMapper;
    private final long publishTimeoutMs;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            OrderEventProducer orderEventProducer,
            ObjectMapper objectMapper,
            @Value("${app.outbox.publish-timeout-ms:130000}") long publishTimeoutMs) {

        this.outboxEventRepository = outboxEventRepository;
        this.orderEventProducer = orderEventProducer;
        this.objectMapper = objectMapper;
        if (publishTimeoutMs <= 0) {
            throw new IllegalArgumentException("Outbox publish timeout must be positive");
        }
        this.publishTimeoutMs = publishTimeoutMs;
    }

    @Scheduled(fixedDelay = 1000)
    public void publishPendingEvents() {

        List<OutboxEvent> pendingEvents =
                outboxEventRepository.findByStatusOrderByCreatedAtAsc("PENDING");

        for (OutboxEvent outboxEvent : pendingEvents) {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            publish(outboxEvent);
        }
    }

    private void publish(OutboxEvent outboxEvent) {
        try {
            log.info("Outbox publish attempt: eventId={}, topic={}, orderId={}",
                    outboxEvent.getEventId(), outboxEvent.getTopicName(), outboxEvent.getOrderId());
            OrderCreatedEvent event = objectMapper.readValue(
                    outboxEvent.getPayload(),
                    OrderCreatedEvent.class
            );

            com.aryan.kafka.avro.OrderCreatedEvent avroEvent =
                    toAvro(event);
            SendResult<String, com.aryan.kafka.avro.OrderCreatedEvent> result =
                    orderEventProducer.publish(avroEvent).get(publishTimeoutMs, TimeUnit.MILLISECONDS);

            outboxEvent.setStatus("PUBLISHED");
            outboxEvent.setPublishedAt(LocalDateTime.now());
            outboxEventRepository.save(outboxEvent);

            log.info(
                    "Outbox event published: eventId={}, orderId={}, topic={}, partition={}, offset={}",
                    outboxEvent.getEventId(),
                    outboxEvent.getOrderId(),
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset()
            );

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            log.error("Outbox publish interrupted; row remains PENDING: eventId={}, topic={}, orderId={}",
                    outboxEvent.getEventId(), outboxEvent.getTopicName(), outboxEvent.getOrderId(), exception);
        } catch (Exception exception) {
            log.error(
                    "Outbox publish failed; row will be retried: eventId={}, topic={}, orderId={}",
                    outboxEvent.getEventId(),
                    outboxEvent.getTopicName(),
                    outboxEvent.getOrderId(),
                    exception
            );
        }
    }
    private com.aryan.kafka.avro.OrderCreatedEvent toAvro(
            OrderCreatedEvent event) {

        return com.aryan.kafka.avro.OrderCreatedEvent.newBuilder()
                .setEventId(event.getEventId())
                .setOrderId(event.getOrderId())
                .setProductId(event.getProductId())
                .setQuantity(event.getQuantity())
                .setAmount(event.getAmount())
                .setStatus(event.getStatus())
                .setOccurredAt(event.getOccurredAt())
                .setSource("WEB")
                .build();
    }
}
