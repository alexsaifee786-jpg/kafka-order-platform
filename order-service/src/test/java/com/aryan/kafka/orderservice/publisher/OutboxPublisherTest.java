package com.aryan.kafka.orderservice.publisher;

import com.aryan.kafka.avro.OrderCreatedEvent;
import com.aryan.kafka.orderservice.entity.OutboxEvent;
import com.aryan.kafka.orderservice.producer.OrderEventProducer;
import com.aryan.kafka.orderservice.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OutboxPublisherTest {
    private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
    private final OrderEventProducer producer = mock(OrderEventProducer.class);
    private final OutboxPublisher publisher = new OutboxPublisher(repository, producer,
            JsonMapper.builder().findAndAddModules().build(), 20);
    private final OutboxEvent row = new OutboxEvent("test-event", 42L, "orders.created.avro",
            "ORDER_CREATED", """
            {"eventId":"test-event","orderId":42,"productId":1,"quantity":1,
             "amount":10.00,"status":"CREATED","occurredAt":"2026-09-28T00:00:00Z"}
            """, "PENDING", LocalDateTime.now());

    @Test
    void savesPublishedOnlyAfterAcknowledgment() {
        when(repository.findByStatusOrderByCreatedAtAsc("PENDING")).thenReturn(List.of(row));
        when(producer.publish(any())).thenAnswer(invocation -> {
            OrderCreatedEvent event = invocation.getArgument(0);
            assertEquals("WEB", event.getSource());
            assertEquals(42L, event.getOrderId());
            assertEquals("PENDING", row.getStatus());
            verify(repository, never()).save(any());
            return CompletableFuture.completedFuture(ack());
        });
        publisher.publishPendingEvents();
        assertEquals("PUBLISHED", row.getStatus());
        assertNotNull(row.getPublishedAt());
        verify(repository).save(row);
    }

    @Test
    void timeoutLeavesPendingAndLaterSchedulerRunRetries() {
        when(repository.findByStatusOrderByCreatedAtAsc("PENDING")).thenReturn(List.of(row));
        CompletableFuture<SendResult<String, OrderCreatedEvent>> stalled = new CompletableFuture<>();
        when(producer.publish(any())).thenReturn(stalled, CompletableFuture.completedFuture(ack()));
        publisher.publishPendingEvents();
        assertPending();
        // A late Kafka acknowledgment must not mark an already timed-out attempt published.
        stalled.complete(ack());
        assertPending();
        publisher.publishPendingEvents();
        assertEquals("PUBLISHED", row.getStatus());
        verify(repository).save(row);
    }

    @Test
    void asynchronousFailureLeavesPending() {
        when(repository.findByStatusOrderByCreatedAtAsc("PENDING")).thenReturn(List.of(row));
        when(producer.publish(any())).thenReturn(CompletableFuture.failedFuture(
                new org.apache.kafka.common.errors.TimeoutException("broker unavailable")));
        publisher.publishPendingEvents();
        assertPending();
    }

    @Test
    void synchronousSerializationFailureLeavesPending() {
        when(repository.findByStatusOrderByCreatedAtAsc("PENDING")).thenReturn(List.of(row));
        when(producer.publish(any())).thenThrow(
                new org.apache.kafka.common.errors.SerializationException("registry unavailable"));
        publisher.publishPendingEvents();
        assertPending();
    }

    @Test
    void interruptionIsPreservedAndStopsBatch() {
        when(repository.findByStatusOrderByCreatedAtAsc("PENDING")).thenReturn(List.of(row, row));
        when(producer.publish(any())).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return new CompletableFuture<SendResult<String, OrderCreatedEvent>>();
        });
        try {
            publisher.publishPendingEvents();
            assertTrue(Thread.currentThread().isInterrupted());
            assertPending();
            verify(producer, times(1)).publish(any());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void emptyPollDoesNotSendOrSave() {
        when(repository.findByStatusOrderByCreatedAtAsc("PENDING")).thenReturn(List.of());
        publisher.publishPendingEvents();
        verifyNoInteractions(producer);
        verify(repository, never()).save(any());
    }

    private void assertPending() {
        assertEquals("PENDING", row.getStatus());
        assertNull(row.getPublishedAt());
        verify(repository, never()).save(any());
    }

    private SendResult<String, OrderCreatedEvent> ack() {
        return new SendResult<>(null, new RecordMetadata(
                new TopicPartition("orders.created.avro", 0), 12L, 0, 0L, 0, 0));
    }
}
