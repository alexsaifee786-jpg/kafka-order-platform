package com.aryan.kafka.inventoryservice.service;
import com.aryan.kafka.inventoryservice.entity.Inventory;
import com.aryan.kafka.inventoryservice.entity.OutboxEvent;
import com.aryan.kafka.inventoryservice.entity.ProcessedEvent;
import com.aryan.kafka.inventoryservice.event.InventoryReservationFailedEvent;
import com.aryan.kafka.inventoryservice.event.InventoryReservedEvent;
import com.aryan.kafka.avro.OrderCreatedEvent;
import com.aryan.kafka.inventoryservice.repository.InventoryRepository;
import com.aryan.kafka.inventoryservice.repository.OutboxEventRepository;
import com.aryan.kafka.inventoryservice.repository.ProcessedEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class InventoryProcessingService {
    private final InventoryRepository inventoryRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private  final String inventoryReservedTopic;

    private final String inventoryReservationFailedTopic;
    public InventoryProcessingService(InventoryRepository inventoryRepository, ProcessedEventRepository processedEventRepository, OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper, @Value("${app.kafka.topics.inventory-reserved}") String inventoryReservedTopic, @Value("${app.kafka.topics.inventory-reservation-failed}")String inventoryReservationFailedTopic) {
        this.inventoryRepository = inventoryRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository=outboxEventRepository;
        this.objectMapper = objectMapper;
        this.inventoryReservedTopic = inventoryReservedTopic;
        this.inventoryReservationFailedTopic = inventoryReservationFailedTopic;
    }
    @Transactional
    public boolean process(
            OrderCreatedEvent event, ConsumerRecord<String,OrderCreatedEvent> record
            )
    {
        boolean alreadyProcessed = processedEventRepository.existsByEventId(event.getEventId());

        if (alreadyProcessed)
        {
            return false;
        }
        Inventory inventory = inventoryRepository.findByProductId(event.getProductId()).orElseThrow(() -> new IllegalArgumentException("Inventory not found for productId=" + event.getProductId()));
        ProcessedEvent processedEvent = new ProcessedEvent(
                event.getEventId(),
                event.getOrderId(),
                record.topic(),
                record.partition(),
                record.offset(),
                LocalDateTime.now()
        );
        processedEventRepository.save(processedEvent);
        if (inventory.getAvailableStock() < event.getQuantity()) {

            InventoryReservationFailedEvent failedEvent =
                    new InventoryReservationFailedEvent(
                            UUID.randomUUID().toString(),
                            event.getEventId(),
                            event.getOrderId(),
                            event.getProductId(),
                            event.getQuantity(),
                            "REJECTED",
                            "Insufficient stock",
                            LocalDateTime.now()
                    );

            try {
                String payload = objectMapper.writeValueAsString(failedEvent);

                OutboxEvent outboxEvent = new OutboxEvent(
                        failedEvent.getEventId(),
                        event.getOrderId(),
                        inventoryReservationFailedTopic,
                        "INVENTORY_RESERVATION_FAILED",
                        payload,
                        "PENDING",
                        LocalDateTime.now()
                );

                outboxEventRepository.save(outboxEvent);
                return true;

            } catch (JsonProcessingException exception) {
                throw new IllegalStateException(
                        "Could not create inventory reservation failed outbox event",
                        exception
                );
            }
        }
        inventory.setAvailableStock(inventory.getAvailableStock()-event.getQuantity());
        inventory.setUpdatedAt(LocalDateTime.now());

        InventoryReservedEvent reservedEvent =
                new InventoryReservedEvent(
                        UUID.randomUUID().toString(),
                        event.getEventId(),
                        event.getOrderId(),
                        event.getProductId(),
                        event.getQuantity(),
                        "RESERVED",
                        LocalDateTime.now()
                );

        try {
            String payload = objectMapper.writeValueAsString(reservedEvent);

            OutboxEvent outboxEvent = new OutboxEvent(
                    reservedEvent.getEventId(),
                    event.getOrderId(),
                    inventoryReservedTopic,
                    "INVENTORY_RESERVED",
                    payload,
                    "PENDING",
                    LocalDateTime.now()
            );

            outboxEventRepository.save(outboxEvent);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException(
                    "Could not create inventory reserved outbox event",
                    exception
            );
        }
     return true;
    }
}
