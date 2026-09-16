package com.aryan.kafka.inventoryservice;

import com.aryan.kafka.inventoryservice.entity.Inventory;
import com.aryan.kafka.inventoryservice.entity.OutboxEvent;
import com.aryan.kafka.avro.OrderCreatedEvent;
import com.aryan.kafka.inventoryservice.repository.InventoryRepository;
import com.aryan.kafka.inventoryservice.repository.OutboxEventRepository;
import com.aryan.kafka.inventoryservice.repository.ProcessedEventRepository;
import com.aryan.kafka.inventoryservice.service.InventoryProcessingService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class InventoryProcessingServiceTest {
    private InventoryRepository inventoryRepository;
    private ProcessedEventRepository processedEventRepository;
    private InventoryProcessingService inventoryProcessingService;
    private OutboxEventRepository outboxEventRepository;
    @BeforeEach
    void setup()
    {
        inventoryRepository  = mock(InventoryRepository.class);
       processedEventRepository= mock(ProcessedEventRepository.class);
        outboxEventRepository = mock(OutboxEventRepository.class);
       inventoryProcessingService=new InventoryProcessingService(inventoryRepository,processedEventRepository,outboxEventRepository,new ObjectMapper().findAndRegisterModules(),"inventory.reserved","inventory.reservation.failed");
    }
    @Test
    void shouldSkipInventoryUpdateWhenAlreadyProcessed()
    {
        OrderCreatedEvent event=new OrderCreatedEvent();
        event.setEventId("evt-4004");
        event.setOrderId(4004L);
        event.setProductId(503L);
        event.setQuantity(10);
        event.setAmount(new BigDecimal("500.00"));
        event.setStatus("CREATED");
        ConsumerRecord<String, OrderCreatedEvent> record = new ConsumerRecord<>("orders.created", 0, 11L, "4004L", event);
   when(processedEventRepository.existsByEventId("evt-4004")).thenReturn(true);
        boolean processed = inventoryProcessingService.process(event, record);
        assertFalse(processed);
        verify(processedEventRepository).existsByEventId("evt-4004");
        verifyNoInteractions(inventoryRepository);
        verify(processedEventRepository,never()).save(any());

    }
    @Test
    void shouldUpdateInventoryWhenEventIsNew() {
        Inventory inventory =
                new Inventory(501L, 10, LocalDateTime.now());

        OrderCreatedEvent event = new OrderCreatedEvent();
        event.setEventId("evt-5001");
        event.setOrderId(5001L);
        event.setProductId(501L);
        event.setQuantity(2);

        ConsumerRecord<String, OrderCreatedEvent> record =
                new ConsumerRecord<>(
                        "orders.created",
                        0,
                        12L,
                        "5001",
                        event
                );

        when(processedEventRepository.existsByEventId("evt-5001"))
                .thenReturn(false);

        when(inventoryRepository.findByProductId(501L))
                .thenReturn(Optional.of(inventory));

        boolean processed = inventoryProcessingService.process(event, record);

        assertTrue(processed);
        assertEquals(8, inventory.getAvailableStock());

        verify(inventoryRepository).findByProductId(501L);
        verify(processedEventRepository).save(any());
    }
    @Test
    void shouldSaveFailureEventWhenStockIsInsufficient() throws Exception  {
        Inventory inventory =
                new Inventory(503L, 5, LocalDateTime.now());

        OrderCreatedEvent event = new OrderCreatedEvent();
        event.setEventId("evt-5002");
        event.setOrderId(5002L);
        event.setProductId(503L);
        event.setQuantity(10);

        ConsumerRecord<String, OrderCreatedEvent> record =
                new ConsumerRecord<>(
                        "orders.created",
                        0,
                        13L,
                        "5002",
                        event
                );

        when(processedEventRepository.existsByEventId("evt-5002"))
                .thenReturn(false);

        when(inventoryRepository.findByProductId(503L))
                .thenReturn(Optional.of(inventory));

        boolean processed = inventoryProcessingService.process(event, record);

// Rejection handle hui, lekin stock reduce nahi hua
        assertTrue(processed);
        assertEquals(5, inventory.getAvailableStock());

// Incoming event handled mark hua
        verify(processedEventRepository).save(any());

// Repository ko save karne ke liye diya gaya OutboxEvent pakdo
        ArgumentCaptor<OutboxEvent> captor =
                ArgumentCaptor.forClass(OutboxEvent.class);

        verify(outboxEventRepository).save(captor.capture());

        OutboxEvent savedEvent = captor.getValue();

        assertEquals(5002L, savedEvent.getOrderId());
        assertEquals("inventory.reservation.failed", savedEvent.getTopicName());
        assertEquals("INVENTORY_RESERVATION_FAILED", savedEvent.getEventType());
        assertEquals("PENDING", savedEvent.getStatus());

// Kafka ko bhejne wale JSON ka content check karo
        JsonNode payload = new ObjectMapper().readTree(savedEvent.getPayload());

        assertEquals(savedEvent.getEventId(), payload.get("eventId").asText());
        assertEquals("evt-5002", payload.get("sourceEventId").asText());
        assertEquals(5002L, payload.get("orderId").asLong());
        assertEquals(503L, payload.get("productId").asLong());
        assertEquals(10, payload.get("quantity").asInt());
        assertEquals("REJECTED", payload.get("status").asText());
        assertEquals("Insufficient stock", payload.get("reason").asText());
    }
}
