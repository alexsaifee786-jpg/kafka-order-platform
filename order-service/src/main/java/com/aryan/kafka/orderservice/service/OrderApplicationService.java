package com.aryan.kafka.orderservice.service;

import com.aryan.kafka.orderservice.dto.CreateOrderRequest;
import com.aryan.kafka.orderservice.entity.Order;
import com.aryan.kafka.orderservice.entity.OutboxEvent;
import com.aryan.kafka.orderservice.entity.ProcessedEvent;
import com.aryan.kafka.orderservice.event.InventoryReservationFailedEvent;
import com.aryan.kafka.orderservice.event.InventoryReservedEvent;
import com.aryan.kafka.orderservice.event.OrderCreatedEvent;
import com.aryan.kafka.orderservice.producer.OrderEventProducer;
import com.aryan.kafka.orderservice.repository.OrderRepository;
import com.aryan.kafka.orderservice.repository.OutboxEventRepository;
import com.aryan.kafka.orderservice.repository.ProcessedEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class OrderApplicationService {
    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final String orderCreatedTopic;

    public OrderApplicationService(OrderEventProducer orderEventProducer, OrderRepository orderRepository, ProcessedEventRepository processedEventRepository, OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper, @Value("${app.kafka.topics.order-created}") String orderCreatedTopic) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.orderCreatedTopic = orderCreatedTopic;
    }
@Transactional
    public Long createOrder(
            CreateOrderRequest request) {
    Order order=new Order(UUID.randomUUID().toString(),request.getAmount(),"pending");
    Order savedOrder = orderRepository.save(order);
    OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                savedOrder.getId(),
                request.getProductId(),
                request.getQuantity(),
                request.getAmount(),
                "CREATED",
                Instant.now()
        );

    try {
        String payload = objectMapper.writeValueAsString(event);

        OutboxEvent outboxEvent = new OutboxEvent(
                event.getEventId(),
                savedOrder.getId(),
                orderCreatedTopic,
                "ORDER_CREATED",
                payload,
                "PENDING",
                LocalDateTime.now()
        );

        outboxEventRepository.save(outboxEvent);

        return savedOrder.getId();
    } catch (JsonProcessingException exception) {
        throw new IllegalStateException(
                "Could not create order-created outbox event",
                exception
        );
    }
    }
    public boolean markInventoryReserved(InventoryReservedEvent event, ConsumerRecord<String,InventoryReservedEvent> record)
    {
        boolean alreadyProcessed = processedEventRepository.existsByEventId(event.getEventId());
        if(alreadyProcessed)
        {
            return false;
        }
        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow(()->new IllegalArgumentException("Order not found for orderId+"+event.getOrderId()));
order.setStatus("INVENTORY_RESERVED");
orderRepository.save(order);
        ProcessedEvent processedEvent=new ProcessedEvent(
                event.getEventId(),
                event.getOrderId(),
                record.topic(),
                record.partition(),
                record.offset(),
                LocalDateTime.now()
        );
        processedEventRepository.save(processedEvent);
        return true;

    }
    @Transactional
    public boolean markInventoryRejected(
            InventoryReservationFailedEvent event,
            ConsumerRecord<String, InventoryReservationFailedEvent> record) {

        boolean alreadyProcessed =
                processedEventRepository.existsByEventId(event.getEventId());

        if (alreadyProcessed) {
            return false;
        }

        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Order not found for orderId=" + event.getOrderId()
                ));

        order.setStatus("INVENTORY_REJECTED");
        orderRepository.save(order);

        ProcessedEvent processedEvent = new ProcessedEvent(
                event.getEventId(),
                event.getOrderId(),
                record.topic(),
                record.partition(),
                record.offset(),
                LocalDateTime.now()
        );

        processedEventRepository.save(processedEvent);
        return true;
    }
}