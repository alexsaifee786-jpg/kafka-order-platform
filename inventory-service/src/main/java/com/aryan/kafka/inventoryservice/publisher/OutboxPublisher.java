package com.aryan.kafka.inventoryservice.publisher;

import com.aryan.kafka.inventoryservice.entity.OutboxEvent;
import com.aryan.kafka.inventoryservice.event.InventoryReservationFailedEvent;
import com.aryan.kafka.inventoryservice.event.InventoryReservedEvent;
import com.aryan.kafka.inventoryservice.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
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
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, Object> kafkaTemplate,
            ObjectMapper objectMapper
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelay = 1000)
    public void publishPendingEvents() {

        List<OutboxEvent> pendingEvents =
                outboxEventRepository
                        .findByStatusOrderByCreatedAtAsc("PENDING");

        for (OutboxEvent outboxEvent : pendingEvents) {
            publish(outboxEvent);
        }
    }

    private void publish(OutboxEvent outboxEvent) {
        try {
            Object event;

            if ("INVENTORY_RESERVED".equals(outboxEvent.getEventType())) {

                event = objectMapper.readValue(
                        outboxEvent.getPayload(),
                        InventoryReservedEvent.class
                );

            } else if ("INVENTORY_RESERVATION_FAILED".equals(
                    outboxEvent.getEventType())) {

                event = objectMapper.readValue(
                        outboxEvent.getPayload(),
                        InventoryReservationFailedEvent.class
                );

            } else {
                throw new IllegalArgumentException(
                        "Unsupported event type: " + outboxEvent.getEventType()
                );
            }

            SendResult<String, Object> result =
                    kafkaTemplate.send(
                            outboxEvent.getTopicName(),
                            String.valueOf(outboxEvent.getOrderId()),
                            event
                    ).get();

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
}