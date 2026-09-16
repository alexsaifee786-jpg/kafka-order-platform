package com.aryan.kafka.orderservice.producer;

import com.aryan.kafka.avro.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class OrderEventProducer {

    private static final Logger log =
            LoggerFactory.getLogger(OrderEventProducer.class);

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
    private final String topicName;

    public OrderEventProducer(
            KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate,
            @Value("${app.kafka.topics.order-created}") String topicName) {

        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public CompletableFuture<SendResult<String, OrderCreatedEvent>> publish(
            OrderCreatedEvent event) {

        String messageKey = String.valueOf(event.getOrderId());

        CompletableFuture<SendResult<String, OrderCreatedEvent>> future =
                kafkaTemplate.send(topicName, messageKey, event);

        future.whenComplete((result, exception) -> {
            if (exception == null) {
                log.info(
                        "Order event published: eventId={}, topic={}, partition={}, offset={}",
                        event.getEventId(),
                        topicName,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset()
                );
            } else {
                log.error(
                        "Failed to publish order event: eventId={}",
                        event.getEventId(),
                        exception
                );
            }
        });

        return future;
    }
}