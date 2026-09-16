package com.aryan.kafka.orderservice.publisher;

import com.aryan.kafka.orderservice.entity.OutboxEvent;
import com.aryan.kafka.orderservice.event.OrderCreatedEvent;
import com.aryan.kafka.orderservice.producer.OrderEventProducer;
import com.aryan.kafka.orderservice.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class OutboxPublisher {

    private static final Logger log =
            LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final OrderEventProducer orderEventProducer;
    private final ObjectMapper objectMapper;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            OrderEventProducer orderEventProducer,
            ObjectMapper objectMapper) {

        this.outboxEventRepository = outboxEventRepository;
        this.orderEventProducer = orderEventProducer;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelay = 1000)
    public void publishPendingEvents() {

        List<OutboxEvent> pendingEvents =
                outboxEventRepository.findByStatusOrderByCreatedAtAsc("PENDING");

        for (OutboxEvent outboxEvent : pendingEvents) {
            publish(outboxEvent);
        }
    }

    private void publish(OutboxEvent outboxEvent) {
        try {
            OrderCreatedEvent event = objectMapper.readValue(
                    outboxEvent.getPayload(),
                    OrderCreatedEvent.class
            );

            com.aryan.kafka.avro.OrderCreatedEvent avroEvent =
                    toAvro(event);
            SendResult<String, com.aryan.kafka.avro.OrderCreatedEvent> result =
                    orderEventProducer.publish(avroEvent).get();

            outboxEvent.setStatus("PUBLISHED");
            outboxEvent.setPublishedAt(LocalDateTime.now());
            outboxEventRepository.save(outboxEvent);

            log.info(
                    "Outbox event published: eventId={}, topic={}, partition={}, offset={}",
                    outboxEvent.getEventId(),
                    outboxEvent.getTopicName(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset()
            );

        } catch (Exception exception) {
            log.error(
                    "Outbox publish failed: eventId={}",
                    outboxEvent.getEventId(),
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